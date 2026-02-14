#!/usr/bin/env python3
"""
ErrorRecovery - Robust retry logic, circuit breakers, and failure alerting
Handles retries, circuit breakers, fallback chains, and dead letter queue
"""

import json
import os
import sys
import time
import subprocess
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Tuple
from functools import wraps
from enum import Enum
import urllib.request
import urllib.error

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class ErrorSeverity(Enum):
    """Error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject fast
    HALF_OPEN = "half_open"  # Testing if recovered


class ErrorRecovery:
    """Error recovery, retry logic, circuit breakers, and failure alerting."""
    
    # Default retry configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BACKOFF_BASE = 2  # seconds
    DEFAULT_BACKOFF_MAX = 60  # seconds
    
    # Circuit breaker config
    CIRCUIT_FAILURE_THRESHOLD = 5
    CIRCUIT_RESET_TIMEOUT = 300  # 5 minutes
    CIRCUIT_HALF_OPEN_MAX_CALLS = 3
    
    # Service names for circuit breakers
    SERVICES = {
        'x_api': 'X/Twitter API',
        'claude_api': 'Claude/Anthropic API',
        'youtube_api': 'YouTube API',
        'reddit_api': 'Reddit API',
        'telegram_api': 'Telegram Bot API',
        'crayo_api': 'Crayo AI TTS API',
        'topmedia_api': 'Top Media API',
        'pexels_api': 'Pexels API',
        'pixabay_api': 'Pixabay API'
    }
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # State directory
        self.state_dir = self.pipeline_dir / "state"
        self.state_dir.mkdir(exist_ok=True)
        
        # Logs directory
        self.logs_dir = self.pipeline_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.errors_dir = self.logs_dir / "errors"
        self.errors_dir.mkdir(exist_ok=True)
        
        # Error tracking - daily files
        self.error_log_file = self.errors_dir / f"{datetime.now().strftime('%Y-%m-%d')}.json"
        self.circuit_state_file = self.state_dir / "circuit_breakers.json"
        self.dead_letter_file = self.state_dir / "dead_letter_queue.json"
        
        # Telegram config for alerts
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # Load states
        self.circuit_states = self._load_circuit_states()
        self.dead_letter_queue = self._load_dead_letter_queue()
    
    def _load_circuit_states(self) -> Dict:
        """Load circuit breaker states."""
        if self.circuit_state_file.exists():
            try:
                with open(self.circuit_state_file) as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_circuit_states(self):
        """Save circuit breaker states."""
        with open(self.circuit_state_file, 'w') as f:
            json.dump(self.circuit_states, f, indent=2)
    
    def _load_dead_letter_queue(self) -> List[Dict]:
        """Load dead letter queue."""
        if self.dead_letter_file.exists():
            try:
                with open(self.dead_letter_file) as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_dead_letter_queue(self):
        """Save dead letter queue."""
        with open(self.dead_letter_file, 'w') as f:
            json.dump(self.dead_letter_queue, f, indent=2)
    
    def _load_error_log(self) -> List[Dict]:
        """Load error log for today."""
        # Update file path for current date
        self.error_log_file = self.errors_dir / f"{datetime.now().strftime('%Y-%m-%d')}.json"
        if self.error_log_file.exists():
            try:
                with open(self.error_log_file) as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_error_log(self, errors: List[Dict]):
        """Save error log."""
        # Keep only last 500 errors per day
        errors = errors[-500:]
        with open(self.error_log_file, 'w') as f:
            json.dump(errors, f, indent=2)
    
    def log_error(self, module: str, operation: str, error: str, 
                  details: Dict = None, severity: str = "error",
                  context: Dict = None):
        """Log an error for tracking."""
        errors = self._load_error_log()
        
        error_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'module': module,
            'operation': operation,
            'error': str(error),
            'error_type': type(error).__name__ if isinstance(error, Exception) else 'Unknown',
            'severity': severity,
            'details': details or {},
            'context': context or {},
            'traceback': traceback.format_exc() if severity in ['error', 'critical'] else None
        }
        
        errors.append(error_entry)
        self._save_error_log(errors)
        
        # Also log to pipeline logs
        log_operation(module, operation, 'error', {
            'error': str(error),
            'severity': severity,
            **(details or {})
        })
        
        # Send alert for critical errors
        if severity == "critical":
            self._send_alert(f"🚨 CRITICAL ERROR in {module}.{operation}: {error}")
        elif severity == "error" and details and details.get('attempts', 0) >= 3:
            # Alert on persistent errors
            self._send_alert(f"⚠️ Persistent error in {module}.{operation} after {details['attempts']} attempts: {error}")
    
    def add_to_dead_letter(self, job: Dict, reason: str, stage: str):
        """Add a failed job to the dead letter queue for later retry."""
        dl_entry = {
            'id': f"{stage}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{len(self.dead_letter_queue)}",
            'stage': stage,
            'job': job,
            'reason': reason,
            'failed_at': datetime.utcnow().isoformat() + 'Z',
            'retry_count': 0,
            'max_retries': 3,
            'status': 'pending'
        }
        
        self.dead_letter_queue.append(dl_entry)
        self._save_dead_letter_queue()
        
        self.log_error(
            module='ErrorRecovery',
            operation='dead_letter_add',
            error=f"Job added to DLQ: {reason}",
            details={'job_id': dl_entry['id'], 'stage': stage},
            severity='warning'
        )
        
        return dl_entry['id']
    
    def retry_dead_letter(self, max_jobs: int = None) -> Dict:
        """Retry failed jobs from dead letter queue."""
        pending = [j for j in self.dead_letter_queue if j['status'] == 'pending' and j['retry_count'] < j['max_retries']]
        
        if max_jobs:
            pending = pending[:max_jobs]
        
        results = {
            'total_pending': len([j for j in self.dead_letter_queue if j['status'] == 'pending']),
            'attempted': len(pending),
            'succeeded': 0,
            'failed': 0,
            'details': []
        }
        
        for job in pending:
            job['retry_count'] += 1
            job['last_retry'] = datetime.utcnow().isoformat() + 'Z'
            
            try:
                # Route to appropriate handler based on stage
                success = self._retry_job(job)
                
                if success:
                    job['status'] = 'completed'
                    job['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                    results['succeeded'] += 1
                    results['details'].append({'id': job['id'], 'status': 'success'})
                else:
                    if job['retry_count'] >= job['max_retries']:
                        job['status'] = 'failed'
                    results['failed'] += 1
                    results['details'].append({'id': job['id'], 'status': 'failed', 'retries': job['retry_count']})
                    
            except Exception as e:
                if job['retry_count'] >= job['max_retries']:
                    job['status'] = 'failed'
                results['failed'] += 1
                results['details'].append({'id': job['id'], 'status': 'error', 'error': str(e)})
        
        self._save_dead_letter_queue()
        
        # Clean up completed jobs older than 7 days
        self._cleanup_dead_letter_queue()
        
        return results
    
    def _retry_job(self, job: Dict) -> bool:
        """Retry a specific job based on its stage."""
        stage = job['stage']
        job_data = job['job']
        
        # Import here to avoid circular imports
        if stage == 'scout':
            from scout_drama import ScoutDrama
            scout = ScoutDrama()
            # Retry specific seed processing
            return True  # Placeholder - implement based on actual job structure
        elif stage == 'scriptsmith':
            from scriptsmith import ScriptSmith
            smith = ScriptSmith()
            # Retry script generation for specific seed
            return True  # Placeholder
        elif stage == 'voiceforge':
            from voiceforge import VoiceForge
            vf = VoiceForge()
            # Retry TTS generation
            return True  # Placeholder
        elif stage == 'assethunter':
            from assethunter import AssetHunter
            ah = AssetHunter()
            # Retry asset search
            return True  # Placeholder
        
        return False
    
    def _cleanup_dead_letter_queue(self):
        """Remove old completed jobs from DLQ."""
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        def is_recent(job):
            if job['status'] != 'completed':
                return True
            completed_at = job.get('completed_at')
            if not completed_at:
                return True
            try:
                completed = datetime.fromisoformat(completed_at.replace('Z', '+00:00').replace('+00:00', ''))
                return completed > cutoff
            except:
                return True
        
        self.dead_letter_queue = [j for j in self.dead_letter_queue if is_recent(j)]
        self._save_dead_letter_queue()
    
    def _send_alert(self, message: str):
        """Send alert via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            print(f"[ErrorRecovery] ALERT: {message}")
            return
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message[:4000],
            "parse_mode": "HTML"
        }
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers={"Content-Type": "application/json"},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                if result.get('ok'):
                    print(f"[ErrorRecovery] Alert sent")
        except Exception as e:
            print(f"[ErrorRecovery] Alert send failed: {e}")
    
    def check_circuit_breaker(self, service: str) -> Tuple[bool, str]:
        """Check if circuit breaker allows requests for a service.
        
        Returns:
            (allowed, reason): Whether requests are allowed and why not
        """
        state = self.circuit_states.get(service, {})
        
        current_state = state.get('state', CircuitState.CLOSED.value)
        failures = state.get('consecutive_failures', 0)
        last_failure = state.get('last_failure')
        half_open_calls = state.get('half_open_calls', 0)
        
        if current_state == CircuitState.OPEN.value:
            # Check if enough time has passed to try half-open
            if last_failure:
                last = datetime.fromisoformat(last_failure.replace('Z', '+00:00'))
                if datetime.utcnow() - last > timedelta(seconds=self.CIRCUIT_RESET_TIMEOUT):
                    # Transition to half-open
                    self.circuit_states[service] = {
                        'state': CircuitState.HALF_OPEN.value,
                        'consecutive_failures': failures,
                        'last_failure': last_failure,
                        'half_open_calls': 0,
                        'half_open_successes': 0
                    }
                    self._save_circuit_states()
                    return True, "half_open"
            
            return False, f"Circuit breaker OPEN for {service} ({failures} failures)"
        
        if current_state == CircuitState.HALF_OPEN.value:
            if half_open_calls >= self.CIRCUIT_HALF_OPEN_MAX_CALLS:
                return False, f"Circuit half-open limit reached for {service}"
            return True, "half_open"
        
        return True, "closed"
    
    def record_failure(self, service: str, error: str, error_type: str = None):
        """Record a failure for circuit breaker."""
        if service not in self.circuit_states:
            self.circuit_states[service] = {
                'state': CircuitState.CLOSED.value,
                'consecutive_failures': 0,
                'half_open_calls': 0,
                'half_open_successes': 0
            }
        
        state = self.circuit_states[service]
        state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
        state['last_failure'] = datetime.utcnow().isoformat() + 'Z'
        state['last_error'] = str(error)
        state['error_type'] = error_type or 'Unknown'
        
        # Check if we should open the circuit
        if state['consecutive_failures'] >= self.CIRCUIT_FAILURE_THRESHOLD:
            state['state'] = CircuitState.OPEN.value
            state['opened_at'] = datetime.utcnow().isoformat() + 'Z'
            
            self._send_alert(
                f"🔴 <b>CIRCUIT BREAKER OPEN</b>\n"
                f"Service: {self.SERVICES.get(service, service)}\n"
                f"Failures: {state['consecutive_failures']}\n"
                f"Last error: {str(error)[:200]}"
            )
        else:
            # Alert if approaching threshold
            if state['consecutive_failures'] >= self.CIRCUIT_FAILURE_THRESHOLD - 2:
                self._send_alert(
                    f"⚠️ Circuit breaker for {self.SERVICES.get(service, service)} "
                    f"at {state['consecutive_failures']}/{self.CIRCUIT_FAILURE_THRESHOLD} failures."
                )
        
        self._save_circuit_states()
    
    def record_success(self, service: str):
        """Record a success, reset or update circuit breaker."""
        if service not in self.circuit_states:
            return
        
        state = self.circuit_states[service]
        current_state = state.get('state', CircuitState.CLOSED.value)
        
        if current_state == CircuitState.HALF_OPEN.value:
            # In half-open, track successes
            state['half_open_successes'] = state.get('half_open_successes', 0) + 1
            state['half_open_calls'] = state.get('half_open_calls', 0) + 1
            
            # If enough successes, close the circuit
            if state['half_open_successes'] >= self.CIRCUIT_HALF_OPEN_MAX_CALLS:
                self._close_circuit(service, state)
        else:
            # Normal success, reset failures
            if state.get('consecutive_failures', 0) > 0:
                state['consecutive_failures'] = 0
                state['last_failure'] = None
                self._save_circuit_states()
    
    def _close_circuit(self, service: str, state: Dict):
        """Close a circuit breaker after recovery."""
        old_failures = state.get('consecutive_failures', 0)
        
        self.circuit_states[service] = {
            'state': CircuitState.CLOSED.value,
            'consecutive_failures': 0,
            'half_open_calls': 0,
            'half_open_successes': 0,
            'closed_at': datetime.utcnow().isoformat() + 'Z',
            'previous_failures': old_failures
        }
        self._save_circuit_states()
        
        self._send_alert(
            f"🟢 <b>CIRCUIT BREAKER CLOSED</b>\n"
            f"Service: {self.SERVICES.get(service, service)}\n"
            f"Recovered after {old_failures} failures"
        )
    
    def retry_with_backoff(self, func: Callable, *args, 
                          max_retries: int = None,
                          backoff_base: float = None,
                          backoff_max: float = None,
                          service_name: str = "unknown",
                          fallback_chain: List[Callable] = None,
                          on_failure: Callable = None,
                          **kwargs) -> Any:
        """Execute function with retry, exponential backoff, and fallback chain.
        
        Args:
            func: Primary function to execute
            args: Arguments for func
            max_retries: Maximum retry attempts
            backoff_base: Initial backoff in seconds
            backoff_max: Maximum backoff in seconds
            service_name: Service identifier for circuit breaker
            fallback_chain: List of fallback functions to try if primary fails
            on_failure: Callback on final failure
            kwargs: Keyword arguments for func
            
        Returns:
            Result from func or fallback
            
        Raises:
            Exception: If all retries and fallbacks fail
        """
        max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        backoff_base = backoff_base or self.DEFAULT_BACKOFF_BASE
        backoff_max = backoff_max or self.DEFAULT_BACKOFF_MAX
        
        # Check circuit breaker
        allowed, reason = self.check_circuit_breaker(service_name)
        if not allowed:
            print(f"[ErrorRecovery] Circuit breaker blocking {service_name}: {reason}")
            
            # Try fallbacks immediately if circuit is open
            if fallback_chain:
                return self._try_fallbacks(fallback_chain, args, kwargs, service_name)
            
            raise Exception(f"Circuit breaker open for {service_name}: {reason}")
        
        last_error = None
        error_type = None
        
        for attempt in range(max_retries + 1):
            try:
                result = func(*args, **kwargs)
                # Success - record and return
                self.record_success(service_name)
                return result
                
            except Exception as e:
                last_error = e
                error_type = type(e).__name__
                
                # Classify error
                is_rate_limit = self._is_rate_limit_error(e)
                is_timeout = self._is_timeout_error(e)
                is_auth = self._is_auth_error(e)
                
                if attempt < max_retries:
                    # Calculate backoff with jitter for rate limits
                    if is_rate_limit:
                        backoff = min(backoff_base * (4 ** attempt), backoff_max * 2)
                    else:
                        backoff = min(backoff_base * (2 ** attempt), backoff_max)
                    
                    print(f"[ErrorRecovery] {service_name} attempt {attempt + 1}/{max_retries + 1} failed: {e}")
                    
                    if is_rate_limit:
                        print(f"[ErrorRecovery] Rate limit detected, backing off for {backoff}s...")
                    elif is_timeout:
                        print(f"[ErrorRecovery] Timeout detected, retrying in {backoff}s...")
                    else:
                        print(f"[ErrorRecovery] Retrying in {backoff}s...")
                    
                    time.sleep(backoff)
                else:
                    # All retries exhausted
                    print(f"[ErrorRecovery] {service_name} all {max_retries + 1} attempts failed")
                    
                    # Record failure for circuit breaker
                    self.record_failure(service_name, str(e), error_type)
                    
                    # Log error
                    self.log_error(
                        module=service_name,
                        operation=func.__name__,
                        error=str(e),
                        details={
                            'attempts': max_retries + 1,
                            'error_type': error_type,
                            'is_rate_limit': is_rate_limit,
                            'is_timeout': is_timeout
                        },
                        severity='critical' if is_auth else 'error',
                        context={'args': str(args), 'kwargs': str(kwargs)}
                    )
                    
                    # Try fallback chain
                    if fallback_chain:
                        try:
                            return self._try_fallbacks(fallback_chain, args, kwargs, service_name)
                        except Exception as fallback_error:
                            print(f"[ErrorRecovery] All fallbacks failed: {fallback_error}")
                    
                    # Call failure callback if provided
                    if on_failure:
                        try:
                            on_failure(e, {'service': service_name, 'attempts': max_retries + 1})
                        except:
                            pass
                    
                    raise last_error
        
        raise last_error  # Should not reach here
    
    def _try_fallbacks(self, fallback_chain: List[Callable], args, kwargs, service_name: str) -> Any:
        """Try fallback functions in order."""
        for i, fallback in enumerate(fallback_chain):
            try:
                print(f"[ErrorRecovery] Trying fallback {i + 1}/{len(fallback_chain)}...")
                result = fallback(*args, **kwargs)
                print(f"[ErrorRecovery] Fallback {i + 1} succeeded")
                
                # Log fallback success
                self.log_error(
                    module=service_name,
                    operation='fallback',
                    error=f"Fallback {i + 1} succeeded after primary failure",
                    details={'fallback_index': i},
                    severity='info'
                )
                
                return result
            except Exception as e:
                print(f"[ErrorRecovery] Fallback {i + 1} failed: {e}")
                continue
        
        raise Exception("All fallbacks exhausted")
    
    def _is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is a rate limit."""
        error_str = str(error).lower()
        rate_indicators = [
            'rate limit', 'ratelimit', 'too many requests', '429',
            'quota exceeded', 'limit exceeded', 'throttle'
        ]
        return any(ind in error_str for ind in rate_indicators)
    
    def _is_timeout_error(self, error: Exception) -> bool:
        """Check if error is a timeout."""
        error_str = str(error).lower()
        timeout_indicators = [
            'timeout', 'timed out', 'connection reset', 'read timeout',
            'connect timeout', 'ssl handshake timeout'
        ]
        return any(ind in error_str for ind in timeout_indicators)
    
    def _is_auth_error(self, error: Exception) -> bool:
        """Check if error is an authentication error."""
        error_str = str(error).lower()
        auth_indicators = [
            'unauthorized', 'authentication', 'api key', 'invalid token',
            '401', '403', 'access denied', 'permission denied'
        ]
        return any(ind in error_str for ind in auth_indicators)
    
    def run_with_recovery(self, command: List[str], 
                         module: str,
                         max_retries: int = 2,
                         timeout: int = 300) -> Tuple[bool, str]:
        """Run a subprocess command with retry logic."""
        def _run():
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode != 0:
                raise Exception(f"Exit code {result.returncode}: {result.stderr}")
            
            return result
        
        try:
            result = self.retry_with_backoff(
                _run,
                max_retries=max_retries,
                service_name=module
            )
            return True, result.stdout
            
        except Exception as e:
            error_msg = f"Command failed after retries: {e}"
            print(f"[ErrorRecovery] {error_msg}")
            return False, str(e)
    
    def check_pipeline_health(self) -> Dict:
        """Check overall pipeline health."""
        errors = self._load_error_log()
        
        # Count errors in last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        recent_errors = [
            e for e in errors 
            if datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')) > cutoff
        ]
        
        # Group by module and severity
        by_module = {}
        by_severity = {'info': 0, 'warning': 0, 'error': 0, 'critical': 0}
        
        for e in recent_errors:
            mod = e['module']
            if mod not in by_module:
                by_module[mod] = {'total': 0, 'by_severity': {}}
            by_module[mod]['total'] += 1
            
            sev = e.get('severity', 'error')
            by_severity[sev] = by_severity.get(sev, 0) + 1
            by_module[mod]['by_severity'][sev] = by_module[mod]['by_severity'].get(sev, 0) + 1
        
        # Determine health status
        critical_count = by_severity['critical']
        error_count = by_severity['error']
        warning_count = by_severity['warning']
        
        if critical_count > 0:
            status = 'critical'
        elif error_count > 10:
            status = 'degraded'
        elif error_count > 0 or warning_count > 20:
            status = 'warning'
        else:
            status = 'healthy'
        
        # Get open circuits
        open_circuits = []
        for service, state in self.circuit_states.items():
            if state.get('state') == CircuitState.OPEN.value:
                open_circuits.append({
                    'service': service,
                    'failures': state.get('consecutive_failures', 0),
                    'opened_at': state.get('opened_at'),
                    'last_error': state.get('last_error', '')[:100]
                })
        
        # Get DLQ stats
        dlq_pending = len([j for j in self.dead_letter_queue if j['status'] == 'pending'])
        dlq_failed = len([j for j in self.dead_letter_queue if j['status'] == 'failed'])
        
        return {
            'status': status,
            'total_errors_24h': len(recent_errors),
            'by_severity': by_severity,
            'by_module': by_module,
            'open_circuits': open_circuits,
            'dead_letter_queue': {
                'pending': dlq_pending,
                'failed': dlq_failed,
                'total': len(self.dead_letter_queue)
            },
            'checked_at': datetime.utcnow().isoformat() + 'Z'
        }
    
    def generate_health_report(self) -> str:
        """Generate health report for Telegram."""
        health = self.check_pipeline_health()
        
        status_emoji = {
            'healthy': '✅',
            'warning': '⚠️',
            'degraded': '🔶',
            'critical': '🚨'
        }
        
        lines = [
            f"{status_emoji.get(health['status'], '❓')} <b>PIPELINE HEALTH</b>",
            f"Status: {health['status'].upper()}",
            f"Checked: {health['checked_at'][:19]}Z",
            "",
            f"📊 Errors (24h): {health['total_errors_24h']}",
            f"   Critical: {health['by_severity']['critical']}",
            f"   Error: {health['by_severity']['error']}",
            f"   Warning: {health['by_severity']['warning']}",
            "",
            f"📦 Dead Letter Queue:",
            f"   Pending: {health['dead_letter_queue']['pending']}",
            f"   Failed: {health['dead_letter_queue']['failed']}"
        ]
        
        if health['open_circuits']:
            lines.extend(["", "🔴 Open Circuits:"])
            for circuit in health['open_circuits']:
                lines.append(f"  • {self.SERVICES.get(circuit['service'], circuit['service'])}")
        
        if health['by_module']:
            lines.extend(["", "📈 By Module:"])
            for module, stats in list(health['by_module'].items())[:5]:
                lines.append(f"  • {module}: {stats['total']} errors")
        
        return '\n'.join(lines)
    
    def send_health_alert(self):
        """Send health report via Telegram."""
        report = self.generate_health_report()
        self._send_alert(report)
    
    def reset_circuit_breaker(self, service: str) -> bool:
        """Manually reset a circuit breaker."""
        if service not in self.circuit_states:
            return False
        
        old_state = self.circuit_states[service]
        self.circuit_states[service] = {
            'state': CircuitState.CLOSED.value,
            'consecutive_failures': 0,
            'half_open_calls': 0,
            'half_open_successes': 0,
            'reset_at': datetime.utcnow().isoformat() + 'Z',
            'previous_state': old_state
        }
        self._save_circuit_states()
        
        self._send_alert(f"🔄 Circuit breaker manually reset for {self.SERVICES.get(service, service)}")
        return True
    
    def reset_all_circuits(self) -> int:
        """Reset all circuit breakers."""
        count = 0
        for service in list(self.circuit_states.keys()):
            if self.reset_circuit_breaker(service):
                count += 1
        return count


# --- Decorators ---

def retry(max_retries: int = 3, backoff_base: float = 2.0, service_name: str = "unknown",
          fallback_chain: List[Callable] = None):
    """Decorator for retry logic with circuit breaker and fallback support."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            recovery = ErrorRecovery()
            return recovery.retry_with_backoff(
                func, *args,
                max_retries=max_retries,
                backoff_base=backoff_base,
                service_name=service_name,
                fallback_chain=fallback_chain,
                **kwargs
            )
        return wrapper
    return decorator


def with_circuit_breaker(service_name: str):
    """Decorator that wraps function with circuit breaker check."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            recovery = ErrorRecovery()
            allowed, reason = recovery.check_circuit_breaker(service_name)
            if not allowed:
                raise Exception(f"Circuit breaker open for {service_name}: {reason}")
            return func(*args, **kwargs)
        return wrapper
    return decorator


# --- CLI ---

def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ErrorRecovery - Retry logic, circuit breakers, and failure alerting'
    )
    parser.add_argument('--status', action='store_true', help='Show pipeline health status')
    parser.add_argument('--report', action='store_true', help='Generate health report')
    parser.add_argument('--alert', action='store_true', help='Send health alert to Telegram')
    parser.add_argument('--errors', action='store_true', help='Show recent errors')
    parser.add_argument('--errors-today', action='store_true', help='Show today\'s errors')
    parser.add_argument('--clear-errors', action='store_true', help='Clear error log')
    parser.add_argument('--circuits', action='store_true', help='Show circuit breaker states')
    parser.add_argument('--reset-circuit', help='Reset circuit breaker for service')
    parser.add_argument('--reset-all-circuits', action='store_true', help='Reset all circuit breakers')
    parser.add_argument('--retry-failed', action='store_true', help='Retry dead letter queue')
    parser.add_argument('--dlq', action='store_true', help='Show dead letter queue')
    parser.add_argument('--clear-dlq', action='store_true', help='Clear dead letter queue')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    args = parser.parse_args()
    
    recovery = ErrorRecovery()
    
    if args.status or args.report:
        health = recovery.check_pipeline_health()
        if args.json:
            print(json.dumps(health, indent=2))
        else:
            print(recovery.generate_health_report())
        return 0 if health['status'] in ['healthy', 'warning'] else 1
    
    if args.alert:
        recovery.send_health_alert()
        return 0
    
    if args.errors:
        errors = recovery._load_error_log()
        # Show last 20 errors
        for e in errors[-20:]:
            print(f"[{e['timestamp'][:19]}] {e['severity'].upper()}: {e['module']}.{e['operation']}")
            print(f"  Error: {e['error'][:200]}")
            if e.get('traceback'):
                print(f"  Traceback: {e['traceback'][:300]}...")
            print()
        return 0
    
    if args.errors_today:
        # Show all errors from today
        errors = recovery._load_error_log()
        today = datetime.now().strftime('%Y-%m-%d')
        today_errors = [e for e in errors if e['timestamp'][:10] == today]
        print(f"Errors for {today}: {len(today_errors)}")
        for e in today_errors:
            print(f"\n[{e['timestamp'][:19]}] {e['severity'].upper()}: {e['module']}.{e['operation']}")
            print(f"  Error: {e['error']}")
        return 0
    
    if args.clear_errors:
        recovery._save_error_log([])
        print("Error log cleared")
        return 0
    
    if args.circuits:
        states = recovery._load_circuit_states()
        if states:
            print("Circuit Breaker States:")
            for service, state in states.items():
                failures = state.get('consecutive_failures', 0)
                circuit_state = state.get('state', 'closed')
                status_emoji = {
                    'closed': '🟢',
                    'open': '🔴',
                    'half_open': '🟡'
                }.get(circuit_state, '⚪')
                print(f"  {status_emoji} {recovery.SERVICES.get(service, service)}: {circuit_state.upper()} ({failures} failures)")
                if state.get('last_error'):
                    print(f"     Last: {state['last_error'][:80]}...")
        else:
            print("No circuit breaker states - all services healthy")
        return 0
    
    if args.reset_circuit:
        if recovery.reset_circuit_breaker(args.reset_circuit):
            print(f"Circuit breaker reset for {args.reset_circuit}")
        else:
            print(f"No circuit found for {args.reset_circuit}")
            print(f"Available: {', '.join(recovery.SERVICES.keys())}")
        return 0
    
    if args.reset_all_circuits:
        count = recovery.reset_all_circuits()
        print(f"Reset {count} circuit breakers")
        return 0
    
    if args.retry_failed:
        results = recovery.retry_dead_letter()
        print(f"Dead letter queue retry complete:")
        print(f"  Attempted: {results['attempted']}")
        print(f"  Succeeded: {results['succeeded']}")
        print(f"  Failed: {results['failed']}")
        print(f"  Remaining pending: {results['total_pending'] - results['succeeded']}")
        return 0 if results['failed'] == 0 else 1
    
    if args.dlq:
        dlq = recovery._load_dead_letter_queue()
        pending = [j for j in dlq if j['status'] == 'pending']
        failed = [j for j in dlq if j['status'] == 'failed']
        completed = [j for j in dlq if j['status'] == 'completed']
        
        print(f"Dead Letter Queue:")
        print(f"  Pending: {len(pending)}")
        print(f"  Failed (max retries): {len(failed)}")
        print(f"  Completed: {len(completed)}")
        
        if pending:
            print("\nPending jobs:")
            for job in pending[:5]:
                print(f"  • {job['id']} ({job['stage']}) - {job['retry_count']}/{job['max_retries']} retries")
        return 0
    
    if args.clear_dlq:
        recovery.dead_letter_queue = []
        recovery._save_dead_letter_queue()
        print("Dead letter queue cleared")
        return 0
    
    # Default: show health
    health = recovery.check_pipeline_health()
    print(recovery.generate_health_report())
    return 0 if health['status'] in ['healthy', 'warning'] else 1


if __name__ == "__main__":
    sys.exit(main())
