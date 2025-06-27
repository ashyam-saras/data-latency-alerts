"""
Slack alerts utility for Airflow DAGs

Handles success/failure notifications with configurable channels and templates.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict

import requests


class SlackAlertsManager:
    """Manages Slack alerts for DAG success/failure notifications."""

    def __init__(self, config_path: str = None):
        """
        Initialize Slack alerts manager.

        Args:
            config_path: Path to slack alerts configuration file
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)

    def _get_default_config_path(self) -> str:
        """Get default configuration file path."""
        # Try to find config relative to DAG file
        dag_dir = Path(__file__).parent.parent
        config_file = dag_dir.parent / "config" / "slack_alerts.json"

        if config_file.exists():
            return str(config_file)

        # Fallback to current directory
        return "config/slack_alerts.json"

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse Slack alerts configuration."""
        try:
            if not os.path.exists(self.config_path):
                self.logger.warning(f"Config file not found: {self.config_path}. Using defaults.")
                return self._get_default_config()

            with open(self.config_path, "r") as f:
                config = json.load(f)

            # Substitute environment variables
            config_str = json.dumps(config)
            config_str = self._substitute_env_vars(config_str)
            return json.loads(config_str)

        except Exception as e:
            self.logger.error(f"Error loading config: {e}. Using defaults.")
            return self._get_default_config()

    def _substitute_env_vars(self, text: str) -> str:
        """Substitute environment variables in configuration text."""
        # Pattern for ${VAR} or ${VAR:-default}
        pattern = r"\$\{([^}]+)\}"

        def replace_var(match):
            var_expr = match.group(1)

            # Handle default values: VAR:-default
            if ":-" in var_expr:
                var_name, default_value = var_expr.split(":-", 1)
                # Handle nested substitution
                if default_value.startswith("${"):
                    default_value = self._substitute_env_vars(default_value)
                return os.getenv(var_name.strip(), default_value)
            else:
                return os.getenv(var_expr, f"${{{var_expr}}}")  # Keep original if not found

        return re.sub(pattern, replace_var, text)

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration when config file is not available."""
        return {
            "slack_alerts": {
                "enabled": True,
                "default_token": os.getenv("SLACK_API_TOKEN", ""),
                "channels": {
                    "success": {
                        "channel_id": os.getenv("SLACK_CHANNEL_ID", ""),
                        "token": os.getenv("SLACK_API_TOKEN", ""),
                        "enabled": True,
                        "message_template": "✅ *Data Latency Check Completed Successfully*\n📊 DAG: `{dag_id}`\n⏰ Execution Date: {execution_date}\n🕐 Duration: {duration}",
                    },
                    "failure": {
                        "channel_id": os.getenv("SLACK_ERROR_CHANNEL_ID", os.getenv("SLACK_CHANNEL_ID", "")),
                        "token": os.getenv("SLACK_API_TOKEN", ""),
                        "enabled": True,
                        "message_template": "🚨 *Data Latency Check Failed*\n📊 DAG: `{dag_id}`\n⏰ Execution Date: {execution_date}\n❌ Failed Task: {failed_task}\n🔍 Error: {error_message}",
                    },
                },
                "notification_settings": {
                    "notify_on_success": True,
                    "notify_on_failure": True,
                    "notify_on_retry": False,
                    "include_logs": True,
                    "include_duration": True,
                },
            }
        }

    def send_success_alert(self, context: Dict[str, Any]) -> None:
        """Send success alert to configured Slack channel."""
        if not self._should_notify("success"):
            return

        config = self.config["slack_alerts"]["channels"]["success"]

        # Prepare message data
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")

        message_data = {
            "dag_id": dag_run.dag_id if dag_run else "unknown",
            "execution_date": dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S UTC") if dag_run else "unknown",
            "duration": self._calculate_duration(context),
            "status": "SUCCESS",
        }

        # Add mentions if configured
        mentions = self._get_mentions("on_success")
        message = config["message_template"].format(**message_data)
        if mentions:
            message = f"{mentions}\n{message}"

        self._send_slack_message(message=message, channel_id=config["channel_id"], token=config["token"])

    def send_failure_alert(self, context: Dict[str, Any]) -> None:
        """Send failure alert to configured Slack channel."""
        if not self._should_notify("failure"):
            return

        config = self.config["slack_alerts"]["channels"]["failure"]

        # Prepare message data
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        exception = context.get("exception")

        message_data = {
            "dag_id": dag_run.dag_id if dag_run else "unknown",
            "execution_date": dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S UTC") if dag_run else "unknown",
            "failed_task": task_instance.task_id if task_instance else "unknown",
            "error_message": str(exception) if exception else "Unknown error",
            "log_url": task_instance.log_url if task_instance else "N/A",
        }

        # Add mentions if configured
        mentions = self._get_mentions("on_failure")
        message = config["message_template"].format(**message_data)
        if mentions:
            message = f"{mentions}\n{message}"

        self._send_slack_message(message=message, channel_id=config["channel_id"], token=config["token"])

    def _should_notify(self, alert_type: str) -> bool:
        """Check if notifications should be sent for the given alert type."""
        try:
            slack_config = self.config["slack_alerts"]

            # Check global enabled flag
            if not slack_config.get("enabled", True):
                return False

            # Check channel-specific enabled flag
            channel_config = slack_config["channels"].get(alert_type, {})
            if not channel_config.get("enabled", True):
                return False

            # Check notification settings
            notify_key = f"notify_on_{alert_type}"
            notification_settings = slack_config.get("notification_settings", {})
            return notification_settings.get(notify_key, True)

        except Exception as e:
            self.logger.error(f"Error checking notification settings: {e}")
            return False

    def _calculate_duration(self, context: Dict[str, Any]) -> str:
        """Calculate and format task duration."""
        try:
            task_instance = context.get("task_instance")
            if not task_instance or not task_instance.start_date or not task_instance.end_date:
                return "N/A"

            duration = task_instance.end_date - task_instance.start_date
            minutes, seconds = divmod(duration.total_seconds(), 60)

            if minutes > 0:
                return f"{int(minutes)}m {int(seconds)}s"
            else:
                return f"{int(seconds)}s"

        except Exception as e:
            self.logger.error(f"Error calculating duration: {e}")
            return "N/A"

    def _get_mentions(self, mention_type: str) -> str:
        """Get user mentions for the specified type."""
        try:
            notification_settings = self.config["slack_alerts"].get("notification_settings", {})
            mention_users = notification_settings.get("mention_users", {})
            users = mention_users.get(mention_type, [])

            if users:
                return " ".join(users)
            return ""

        except Exception as e:
            self.logger.error(f"Error getting mentions: {e}")
            return ""

    def _send_slack_message(self, message: str, channel_id: str, token: str) -> None:
        """Send message to Slack channel."""
        try:
            if not channel_id or not token:
                self.logger.error("Missing Slack channel ID or token")
                return

            url = "https://slack.com/api/chat.postMessage"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            payload = {"channel": channel_id, "text": message, "mrkdwn": True}

            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()

            result = response.json()
            if not result.get("ok"):
                self.logger.error(f"Slack API error: {result.get('error', 'Unknown error')}")
            else:
                self.logger.info("Slack notification sent successfully")

        except Exception as e:
            self.logger.error(f"Error sending Slack message: {e}")


# Global instance for easy access
slack_alerts = SlackAlertsManager()


# Callback functions for Airflow
def on_success_callback(context):
    """Success callback for Airflow tasks."""
    slack_alerts.send_success_alert(context)


def on_failure_callback(context):
    """Failure callback for Airflow tasks."""
    slack_alerts.send_failure_alert(context)
