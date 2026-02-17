#!/usr/bin/env python3
"""
ScoutDrama - Drama Seed Detection Module
Fetches drama seeds from X/Twitter and Reddit, validates them through 5 gates,
calculates priority scores, and outputs ranked seed cards.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import urllib.request
import urllib.error

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation
from scout_instagram import fetch_all_instagram_posts


class ScoutDrama:
    """Drama seed detection and validation agent."""
    
    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.seeds_dir.mkdir(exist_ok=True)
        
        # API credentials
        self.x_bearer_token = os.environ.get('X_BEARER_TOKEN') or self.config.get('x_api', {}).get('bearer_token', '')
        
        # Thresholds
        self.min_x_engagement = self.config.get('thresholds', {}).get('min_x_engagement', 100)
        self.min_reddit_score = self.config.get('thresholds', {}).get('min_reddit_score', 500)
        
        # Scoring weights
        scoring = self.config.get('scoring', {})
        self.conflict_weight = scoring.get('conflict_weight', 0.3)
        self.viral_weight = scoring.get('viral_weight', 0.4)
        self.time_weight = scoring.get('time_weight', 0.3)
    
    def fetch_x_posts(self, user_id: str, handle: str) -> List[Dict]:
        """Fetch recent posts from X/Twitter user."""
        if not self.x_bearer_token:
            print(f"[ScoutDrama] Warning: X_BEARER_TOKEN not set, skipping {handle}")
            return []
        
        # Calculate 24h ago in ISO format
        start_time = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        url = f"https://api.twitter.com/2/users/{user_id}/tweets?max_results=10&tweet.fields=created_at,public_metrics,text&start_time={start_time}"
        
        headers = {
            "Authorization": f"Bearer {self.x_bearer_token}",
            "User-Agent": "DramaPipeline/1.0"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                
                posts = []
                for tweet in data.get('data', []):
                    metrics = tweet.get('public_metrics', {})
                    likes = metrics.get('like_count', 0)
                    retweets = metrics.get('retweet_count', 0)
                    replies = metrics.get('reply_count', 0)
                    
                    engagement = likes + (retweets * 2) + (replies * 3)
                    
                    posts.append({
                        'id': tweet['id'],
                        'text': tweet['text'],
                        'created_at': tweet['created_at'],
                        'source': handle,
                        'source_url': f"https://x.com/{handle.lstrip('@')}/status/{tweet['id']}",
                        'likes': likes,
                        'retweets': retweets,
                        'replies': replies,
                        'engagement': engagement
                    })
                
                print(f"[ScoutDrama] Fetched {len(posts)} posts from {handle}")
                return posts
                
        except urllib.error.HTTPError as e:
            print(f"[ScoutDrama] X API error for {handle}: {e.code} - {e.reason}")
            return []
        except Exception as e:
            print(f"[ScoutDrama] Error fetching {handle}: {e}")
            return []
    
    def fetch_reddit_posts(self, subreddit: str) -> List[Dict]:
        """Fetch hot posts from Reddit subreddit."""
        url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit=5"
        
        headers = {
            "User-Agent": "DramaPipeline/1.0 (by /u/dramapipeline)"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                
                posts = []
                for child in data.get('data', {}).get('children', []):
                    post = child.get('data', {})
                    score = post.get('score', 0)
                    num_comments = post.get('num_comments', 0)
                    engagement = score + (num_comments * 2)
                    
                    posts.append({
                        'id': post['id'],
                        'title': post['title'],
                        'text': post.get('selftext', ''),
                        'url': f"https://reddit.com{post.get('permalink', '')}",
                        'created_utc': post.get('created_utc', 0),
                        'source': f"r/{subreddit}",
                        'score': score,
                        'num_comments': num_comments,
                        'engagement': engagement
                    })
                
                print(f"[ScoutDrama] Fetched {len(posts)} posts from r/{subreddit}")
                return posts
                
        except urllib.error.HTTPError as e:
            print(f"[ScoutDrama] Reddit API error for r/{subreddit}: {e.code} - {e.reason}")
            return []
        except Exception as e:
            print(f"[ScoutDrama] Error fetching r/{subreddit}: {e}")
            return []
    
    def calculate_time_bonus(self, created_at: str) -> int:
        """Calculate time freshness bonus (1-10 scale)."""
        try:
            # Parse timestamp
            if 'T' in created_at:
                # ISO format from X
                post_time = datetime.fromisoformat(created_at.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                # Unix timestamp from Reddit
                post_time = datetime.fromtimestamp(created_at, tz=timezone.utc).replace(tzinfo=None)
            
            age_hours = (datetime.now(timezone.utc) - post_time).total_seconds() / 3600
            
            if age_hours < 2:
                return 10  # Breaking
            elif age_hours < 12:
                return 7   # Fresh
            elif age_hours < 24:
                return 4   # Warm
            else:
                return 1   # Old
        except:
            return 1
    
    def detect_emotional_trigger(self, text: str) -> tuple:
        """Detect primary emotional trigger from text."""
        text_lower = text.lower()
        
        # Keywords for each emotion
        emotion_keywords = {
            'outrage': ['outraged', 'furious', 'angry', 'mad', 'pissed', 'cancel', 'boycott', 'disgusting', 'shame', 'disgraceful', 'unacceptable'],
            'surprise': ['shocked', 'surprised', 'unexpected', 'never saw', 'crazy', 'insane', 'wild', 'unbelievable', 'omg', 'wtf'],
            'disbelief': ['can\'t believe', 'hard to believe', 'no way', 'actually happened', 'really did', 'seriously'],
            'empathy': ['sad', 'heartbreaking', 'feel bad', 'poor', 'devastated', 'struggling', 'mental health', 'support'],
            'humor': ['funny', 'hilarious', 'laughing', 'lol', 'lmao', 'comedy', 'joke', 'meme', 'savage'],
            'betrayal': ['betrayed', 'cheated', 'lied', 'backstabbed', 'exposed', 'leaked', 'secret', 'hidden']
        }
        
        scores = {}
        for emotion, keywords in emotion_keywords.items():
            scores[emotion] = sum(1 for kw in keywords if kw in text_lower)
        
        if max(scores.values()) > 0:
            primary_emotion = max(scores, key=scores.get)
            return primary_emotion, True
        
        return 'neutral', False
    
    def extract_key_figures(self, text: str) -> List[str]:
        """Extract potential key figures/celebrities from text."""
        # Simple pattern: Capitalized words that look like names
        # More sophisticated NER would require additional libraries
        words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
        
        # Filter out common non-name capitalized words
        stop_words = {'The', 'This', 'That', 'These', 'Those', 'They', 'Their', 'There', 'Then', 'Than',
                      'What', 'When', 'Where', 'Why', 'How', 'Who', 'Which', 'While', 'With', 'Without',
                      'And', 'But', 'Or', 'Nor', 'For', 'So', 'Yet', 'Because', 'Since', 'Until',
                      'Although', 'Though', 'Unless', 'Whether', 'Before', 'After', 'Above', 'Below',
                      'Between', 'Among', 'During', 'Inside', 'Outside', 'Into', 'Onto', 'Upon',
                      'About', 'Against', 'Around', 'Behind', 'Beyond', 'Except', 'Regarding',
                      'Despite', 'Throughout', 'Toward', 'Towards', 'Within', 'Without',
                      'New', 'Old', 'Good', 'Bad', 'Big', 'Small', 'First', 'Last', 'Next', 'Best',
                      'Real', 'True', 'Sure', 'Just', 'Only', 'Even', 'Also', 'Still', 'Already',
                      'Always', 'Never', 'Often', 'Sometimes', 'Usually', 'Maybe', 'Probably',
                      'Actually', 'Basically', 'Literally', 'Definitely', 'Absolutely', 'Totally',
                      'Very', 'Really', 'Quite', 'Pretty', 'Rather', 'Fairly', 'Extremely',
                      'Twitter', 'Reddit', 'Instagram', 'YouTube', 'TikTok', 'Facebook',
                      'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
                      'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                      'September', 'October', 'November', 'December'}
        
        names = [w for w in words if w not in stop_words and len(w) > 2]
        
        # Return unique names, limit to top 5
        seen = set()
        unique_names = []
        for name in names:
            if name not in seen:
                seen.add(name)
                unique_names.append(name)
                if len(unique_names) >= 5:
                    break
        
        return unique_names
    
    def assess_risk_level(self, text: str) -> tuple:
        """Assess legal/reputation risk level."""
        text_lower = text.lower()
        
        high_risk_keywords = ['lawsuit', 'suicide', 'death threat', 'violence', 'abuse', 'assault', 
                              'arrested', 'charged', 'criminal', 'illegal', 'minor', 'underage']
        medium_risk_keywords = ['controversy', 'drama', 'feud', 'beef', 'exposed', 'leaked', 'rumor',
                                'allegedly', 'reportedly', 'claimed', 'accused']
        
        high_count = sum(1 for kw in high_risk_keywords if kw in text_lower)
        medium_count = sum(1 for kw in medium_risk_keywords if kw in text_lower)
        
        if high_count > 0:
            return 'high', f"Contains high-risk keywords: {high_count} detected. Use 'reportedly/allegedly' language."
        elif medium_count > 0:
            return 'medium', f"Contains medium-risk keywords: {medium_count} detected. Verify sources."
        else:
            return 'low', ''
    
    def create_seed_card(self, post: Dict, source_type: str) -> Optional[Dict]:
        """Create a narrative seed card from a post."""
        # Get text content
        if source_type == 'x':
            text = post['text']
            headline = text[:200] if len(text) > 200 else text
        else:  # reddit
            text = f"{post['title']} {post['text']}"
            headline = post['title']
        
        # Detect emotional trigger
        emotional_trigger, has_emotion = self.detect_emotional_trigger(text)
        
        # Extract key figures
        key_figures = self.extract_key_figures(text)
        
        # Assess risk
        risk_level, risk_notes = self.assess_risk_level(text)
        
        # Calculate time bonus
        if source_type == 'x':
            created_at = post['created_at']
        else:
            created_at = post['created_utc']
        
        time_bonus = self.calculate_time_bonus(created_at)
        
        # Estimate conflict and viral potential (1-10 scale based on engagement)
        engagement = post['engagement']
        if engagement > 10000:
            viral_potential = 10
            conflict_level = 8
        elif engagement > 5000:
            viral_potential = 8
            conflict_level = 7
        elif engagement > 1000:
            viral_potential = 6
            conflict_level = 5
        else:
            viral_potential = 4
            conflict_level = 3
        
        # Calculate priority score
        priority_score = round(
            (conflict_level * self.conflict_weight) +
            (viral_potential * self.viral_weight) +
            (time_bonus * self.time_weight),
            1
        )
        
        # Build narrative angle
        narrative_angle = f"A story about {headline[:100]}... that evokes {emotional_trigger}."
        
        # Create seed card
        date_str = datetime.now().strftime("%Y%m%d")
        seed_id = f"seed-{date_str}-{post['id'][:8]}"
        
        seed = {
            "id": seed_id,
            "headline": headline,
            "source_urls": [post['source_url'] if source_type == 'x' else post['url']],
            "source": post['source'],
            "conflict_level": conflict_level,
            "viral_potential": viral_potential,
            "emotional_trigger": emotional_trigger,
            "time_sensitivity": "high" if time_bonus >= 7 else "medium" if time_bonus >= 4 else "low",
            "key_figures": key_figures,
            "narrative_angle": narrative_angle,
            "risk_level": risk_level,
            "risk_notes": risk_notes,
            "context": text[:2000],
            "screenshots": [],
            "raw_clips": [],
            "engagement": engagement,
            "priority_score": priority_score,
            "validation_gate": {
                "proof_of_concept": False,  # Set in validation
                "emotional_trigger_identified": has_emotion,
                "visual_appeal": False,  # Set in validation
                "broad_audience": len(key_figures) > 0,
                "story_extractable": len(headline) > 20,
                "passed": False  # Set after all validations
            },
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        
        return seed
    
    def validate_seed(self, seed: Dict) -> Dict:
        """Run seed through 5-gate validation."""
        gate = seed['validation_gate']
        
        # Gate 1: Proof of Concept (engagement threshold)
        engagement = seed['engagement']
        if seed['source'].startswith('@'):
            gate['proof_of_concept'] = engagement >= self.min_x_engagement
        else:
            gate['proof_of_concept'] = engagement >= self.min_reddit_score
        
        # Gate 2: Emotional Trigger (already set in create_seed_card)
        # gate['emotional_trigger_identified'] already set
        
        # Gate 3: Visual Appeal
        # For now, assume screenshots can be captured from source URLs
        gate['visual_appeal'] = len(seed['source_urls']) > 0
        
        # Gate 4: Broad Audience (already set in create_seed_card)
        # gate['broad_audience'] already set
        
        # Gate 5: Story Extractable (already set in create_seed_card)
        # gate['story_extractable'] already set
        
        # All gates must pass
        gate['passed'] = all([
            gate['proof_of_concept'],
            gate['emotional_trigger_identified'],
            gate['visual_appeal'],
            gate['broad_audience'],
            gate['story_extractable']
        ])
        
        seed['validation_gate'] = gate
        return seed
    
    def run(self) -> Dict:
        """Run full ScoutDrama scan."""
        start_time = datetime.now(timezone.utc)
        print(f"[ScoutDrama] Starting scan at {start_time.isoformat()}Z")
        
        all_posts = []
        
        # Fetch from X sources
        x_sources = self.config.get('sources', {}).get('x', [])
        for source in x_sources:
            posts = self.fetch_x_posts(source['user_id'], source['handle'])
            all_posts.extend([{**p, '_type': 'x'} for p in posts])
        
        # Fetch from Reddit sources
        reddit_sources = self.config.get('sources', {}).get('reddit', [])
        for source in reddit_sources:
            posts = self.fetch_reddit_posts(source['subreddit'])
            all_posts.extend([{**p, '_type': 'reddit'} for p in posts])
        
        # Fetch from Instagram sources
        ig_posts = fetch_all_instagram_posts(self.config)
        all_posts.extend(ig_posts)
        
        print(f"[ScoutDrama] Total posts fetched: {len(all_posts)}")
        
        # Create and validate seed cards
        seeds = []
        for post in all_posts:
            seed = self.create_seed_card(post, post['_type'])
            if seed:
                seed = self.validate_seed(seed)
                if seed['validation_gate']['passed']:
                    seeds.append(seed)
        
        # Sort by priority score descending
        seeds.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Build output
        scan_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        output = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "scan_timestamp": start_time.isoformat().replace("+00:00", "Z"),
            "source_stats": {
                "x_tweets_fetched": len([p for p in all_posts if p['_type'] == 'x']),
                "reddit_posts_fetched": len([p for p in all_posts if p['_type'] == 'reddit']),
                "instagram_posts_fetched": len([p for p in all_posts if p['_type'] == 'instagram']),
                "rss_items_fetched": 0
            },
            "seeds": seeds,
            "filtered_count": len(all_posts) - len(seeds),
            "scan_duration_sec": round(scan_duration, 2)
        }
        
        # Save to file
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_file = self.seeds_dir / f"{date_str}.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        # Generate summary
        self._generate_summary(output, date_str)
        
        print(f"[ScoutDrama] Scan complete. {len(seeds)} seeds passed validation.")
        print(f"[ScoutDrama] Output saved to: {output_file}")
        
        log_operation('ScoutDrama', 'scan', 'success', {
            'seeds_found': len(seeds),
            'seeds_filtered': output['filtered_count'],
            'duration_sec': scan_duration
        })
        
        return output
    
    def _generate_summary(self, output: Dict, date_str: str):
        """Generate human-readable summary."""
        summary_file = self.seeds_dir / f"{date_str}-summary.md"
        
        lines = [
            f"# Drama Seeds Summary - {date_str}",
            "",
            f"**Scan Time:** {output['scan_timestamp']}",
            f"**Duration:** {output['scan_duration_sec']}s",
            f"**Seeds Found:** {len(output['seeds'])}",
            f"**Filtered Out:** {output['filtered_count']}",
            "",
            "## Source Stats",
            f"- X/Twitter: {output['source_stats']['x_tweets_fetched']} tweets",
            f"- Reddit: {output['source_stats']['reddit_posts_fetched']} posts",
            f"- Instagram: {output['source_stats']['instagram_posts_fetched']} posts",
            "",
            "## Top Seeds",
            ""
        ]
        
        for i, seed in enumerate(output['seeds'][:10], 1):
            lines.extend([
                f"### {i}. {seed['headline'][:80]}...",
                f"- **Source:** {seed['source']}",
                f"- **Priority Score:** {seed['priority_score']}/10",
                f"- **Emotion:** {seed['emotional_trigger']}",
                f"- **Risk:** {seed['risk_level']}",
                f"- **Engagement:** {seed['engagement']:,}",
                f"- **URL:** {seed['source_urls'][0]}",
                ""
            ])
        
        summary_file.write_text('\n'.join(lines))
        print(f"[ScoutDrama] Summary saved to: {summary_file}")


def main():
    """CLI entry point."""
    scout = ScoutDrama()
    result = scout.run()
    
    print(f"\n{'='*50}")
    print(f"ScoutDrama Complete")
    print(f"{'='*50}")
    print(f"Seeds found: {len(result['seeds'])}")
    print(f"Top priority: {result['seeds'][0]['priority_score'] if result['seeds'] else 'N/A'}")
    
    return 0 if result['seeds'] else 1


if __name__ == "__main__":
    sys.exit(main())
