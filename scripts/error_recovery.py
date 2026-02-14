#!/usr/bin/env python3
"""
ErrorRecovery - Robust retry logic and failure alerting
Handles retries, circuit breakers, and failure notifications
"""

import json
import os
import sys
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from functools import wraps
import urllib.request

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class ErrorRecovery:
    """Error recovery, retry logic, and failure alerting."""
    
    # Default retry configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BACKOFF_BASE = 2  # seconds
    DEFAULT_BACKOFF_MAX = 60  # seconds
    
    # Circuit breaker config
    CIRCUIT_FAILURE_THRESHOLD = 5
    CIRCUIT_RESET_TIMEOUT = 300  # 5 minutes
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # State directory
        self.state_dir = self.pipeline_dir / "state"
        self.state_dir.mkdir(exist_ok=True)
        
        # Error tracking
        self.error_log_file = self.state_dir / "error_log.json"
        self.circuit_state_file = self.state_dir / "circuit_breakers.json"
        
        # Telegram config for alerts
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # Load circuit breaker states
        self.circuit_states = self._load_circuit_states()
    
    def _load_circuit_states(self) -> Dict:
        """Load circuit breaker states."""
        if self.circuit_state_file.exists():
            with open(self.circuit_state_file) as f:
                return json.load(f)
        return {}
    
    def _save_circuit_states(self):
        """Save circuit breaker states."""
        with open(self.circuit_state_file, 'w') as f:
            json.dump(self.circuit_states, f, indent=2)
    
    def _load_error_log(self) -> List[Dict]:
        """Load error log."""
        if self.error_log_file.exists():
            with open(self.error_log_file) as f:
                return json.load(f)
        return []
    
    def _save_error_log(self, errors: List[Dict]):
        """Save error log."""
        # Keep only last 100 errors
        errors = errors[-100:]
        with open(self.error_log_file, 'w') as f:
            json.dump(errors, f, indent=2)
    
    def log_error(self, module: str, operation: str, error: str, 
                  details: Dict = None, severity: str = "error"):
        """Log an error for tracking."""
        errors = self._load_error_log()
        
        error_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'module': module,
            'operation': operation,
            'error': str(error),
            'severity': severity,
            'details': details or {}
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
    
    def check_circuit_breaker(self, service: str) -> bool:
        """Check if circuit breaker is open for a service."""
        state = self.circuit_states.get(service, {})
        
        if not state:
            return True  # Circuit is closed (OK)
        
        failures = state.get('consecutive_failures', 0)
        last_failure = state.get('last_failure')
        
        if failures >= self.CIRCUIT_FAILURE_THRESHOLD:
            # Check if enough time has passed to try again
            if last_failure:
                last = datetime.fromisoformat(last_failure.replace('Z', '+00:00'))
                if datetime.now() - last > timedelta(seconds=self.CIRCUIT_RESET_TIMEOUT):
                    # Reset circuit
                    self.circuit_states[service] = {'consecutive_failures': 0}
                    self._save_circuit_states()
                    return True
            
            print(f"[ErrorRecovery] Circuit breaker OPEN for {service}")
            return False
        
        return True
    
    def record_failure(self, service: str, error: str):
        """Record a failure for circuit breaker."""
        if service not in self.circuit_states:
            self.circuit_states[service] = {}
        
        state = self.circuit_states[service]
        state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
        state['last_failure'] = datetime.utcnow().isoformat() + 'Z'
        state['last_error'] = str(error)
        
        self._save_circuit_states()
        
        # Alert if circuit is about to open
        if state['consecutive_failures'] >= self.CIRCUIT_FAILURE_THRESHOLD - 1:
            self._send_alert(
                f"⚠️ Circuit breaker for {service} at {state['consecutive_failures']} failures. "
                f"Will open at {self.CIRCUIT_FAILURE_THRESHOLD}."
            )
    
    def record_success(self, service: str):
        """Record a success, reset circuit breaker."""
        if service in self.circuit_states:
            del self.circuit_states[service]
            self._save_circuit_states()
    
    def retry_with_backoff(self, func: Callable, *args, 
                          max_retries: int = None,
                          backoff_base: float = None,
                          backoff_max: float = None,
                          service_name: str = "unknown",
                          **kwargs) -> Any:
        """Execute function with retry and exponential backoff."""
        max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        backoff_base = backoff_base or self.DEFAULT_BACKOFF_BASE
        backoff_max = backoff_max or self.DEFAULT_BACKOFF_MAX
        
        # Check circuit breaker
        if not self.check_circuit_breaker(service_name):
            raise Exception(f"Circuit breaker open for {service_name}")
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                result = func(*args, **kwargs)
                # Success - reset circuit breaker
                self.record_success(service_name)
                return result
                
            except Exception as e:
                last_error = e
                
                if attempt < max_retries:
                    # Calculate backoff
                    backoff = min(backoff_base * (2 ** attempt), backoff_max)
                    
                    print(f"[ErrorRecovery] {service_name} attempt {attempt + 1} failed: {e}")
                    print(f"[ErrorRecovery] Retrying in {backoff}s...")
                    
                    time.sleep(backoff)
                else:
                    # All retries exhausted
                    print(f"[ErrorRecovery] {service_name} all {max_retries + 1} attempts failed")
                    
                    # Record failure for circuit breaker
                    self.record_failure(service_name, str(e))
                    
                    # Log error
                    self.log_error(
                        module=service_name,
                        operation=func.__name__,
                        error=str(e),
                        details={'attempts': max_retries + 1},
                        severity='critical' if max_retries > 2 else 'error'
                    )
                    
                    raise last_error
        
        raise last_error  # Should not reach here
    
    def run_with_recovery(self, command: List[str], 
                         module: str,
                         max_retries: int = 2,
                         timeout: int = 300) -> bool:
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
            print(result.stdout)
            return True
            
        except Exception as e:
            print(f"[ErrorRecovery] Command failed after retries: {e}")
            return False
    
    def check_pipeline_health(self) -> Dict:
        """Check overall pipeline health."""
        errors = self._load_error_log()
        
        # Count errors in last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        recent_errors = [
            e for e in errors 
            if datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')) > cutoff
        ]
        
        # Group by module
        by_module = {}
        for e in recent_errors:
            mod = e['module']
            if mod not in by_module:
                by_module[mod] = []
            by_module[mod].append(e)
        
        # Determine health status
        critical_count = sum(1 for e in recent_errors if e['severity'] == 'critical')
        error_count = len(recent_errors)
        
        if critical_count > 0:
            status = 'critical'
        elif error_count > 10:
            status = 'degraded'
        elif error_count > 0:
            status = 'warning'
        else:
            status = 'healthy'
        
        return {
            'status': status,
            'total_errors_24h': error_count,
            'critical_errors': critical_count,
            'by_module': {m: len(e) for m, e in by_module.items()},
            'open_circuits': [
                s for s, state in self.circuit_states.items()
                if state.get('consecutive_failures', 0) >= self.CIRCUIT_FAILURE_THRESHOLD
            ],
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
            f"Errors (24h): {health['total_errors_24h']}",
            f"Critical: {health['critical_errors']}"
        ]
        
        if health['by_module']:
            lines.extend(["", "By Module:"])
            for module, count in health['by_module'].items():
                lines.append(f"  • {module}: {count}")
        
        if health['open_circuits']:
            lines.extend(["", "🔴 Open Circuits:"])
            for circuit in health['open_circuits']:
                lines.append(f"  • {circuit}")
        
        return '\n'.join(lines)
    
    def send_health_alert(self):
        """Send health report via Telegram."""
        report = self.generate_health_report()
        self._send_alert(report)


def retry(max_retries: int = 3, backoff_base: float = 2.0, service_name: str = "unknown"):
    """Decorator for retry logic."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            recovery = ErrorRecovery()
            return recovery.retry_with_backoff(
                func, *args,
                max_retries=max_retries,
                backoff_base=backoff_base,
                service_name=service_name,
                **kwargs
            )
        return wrapper
    return decorator


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ErrorRecovery - Retry logic and alerting')
    parser.add_argument('--health', action='store_true', help='Check pipeline health')
    parser.add_argument('--report', action='store_true', help='Generate health report')
    parser.add_argument('--alert', action='store_true', help='Send health alert')
    parser.add_argument('--errors', action='store_true', help='Show recent errors')
    parser.add_argument('--clear-errors', action='store_true', help='Clear error log')
    parser.add_argument('--circuits', action='store_true', help='Show circuit breaker states')
    parser.add_argument('--reset-circuit', help='Reset circuit breaker for service')
    
    args = parser.parse_args()
    
    recovery = ErrorRecovery()
    
    if args.health:
        health = recovery.check_pipeline_health()
        print(json.dumps(health, indent=2))
        return 0
    
    if args.report:
        report = recovery.generate_health_report()
        print(report)
        return 0
    
    if args.alert:
        recovery.send_health_alert()
        return 0
    
    if args.errors:
        errors = recovery._load_error_log()
        # Show last 20 errors
        for e in errors[-20:]:
            print(f"[{e['timestamp'][:19]}] {e['severity'].upper()}: {e['module']}.{e['operation']}")
            print(f"  Error: {e['error']}")
            print()
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
                status = "🔴 OPEN" if failures >= recovery.CIRCUIT_FAILURE_THRESHOLD else "🟢 CLOSED"
                print(f"  {service}: {status} ({failures} failures)")
        else:
            print("No circuit breaker states")
        return 0
    
    if args.reset_circuit:
        if args.reset_circuit in recovery.circuit_states:
            del recovery.circuit_states[args.reset_circuit]
            recovery._save_circuit_states()
            print(f"Circuit breaker reset for {args.reset_circuit}")
        else:
            print(f"No circuit found for {args.reset_circuit}")
        return 0
    
    # Default: show health
    health = recovery.check_pipeline_health()
    print(recovery.generate_health_report())
    
    return 0 if health['status'] in ['healthy', 'warning'] else 1


if __name__ == "__main__":
    sys.exit(main())
