import os
from dataclasses import dataclass


@dataclass
class Config:
    on_prem_subfolder: str
    project_id: str
    topic_name: str
    env: str

    @classmethod
    def from_env(cls):
        return cls(
            on_prem_subfolder=os.getenv("ON-PREM-SUBFOLDER"),
            project_id=os.getenv("PROJECT_ID"),
            topic_name=os.getenv("TOPIC_NAME"),
            env=os.getenv("ENV"),
        )

    def log(self):
        print(f"Configuration: Project ID: {self.project_id}")
        print(f"Configuration: Topic Name: {self.topic_name}")
        print(f"Configuration: ON-PREM-SUBFOLDER: {self.on_prem_subfolder}")
        print(f"Configuration: Env: {self.env}")
