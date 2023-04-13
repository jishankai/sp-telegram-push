import yaml
import dotenv
from pathlib import Path

config_dir = Path(__file__).parent.parent.resolve() / "config"

# load yaml config
with open(config_dir / "config.yml", 'r') as f:
    config_yaml = yaml.safe_load(f)

# config parameters
telegram_token = config_yaml["telegram_token"]
bot_id = config_yaml["bot_id"]
group_chat_id = config_yaml["group_chat_id"]
midas_group_chat_id = config_yaml["midas_group_chat_id"]
