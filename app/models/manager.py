import json
from .GlobalConfig import GlobalConfig


settings = GlobalConfig()

print(f'settings:\n{json.dumps(settings.dict(), indent=2)}')



