# dbt_transform.py

from dbt.compilation import Compiler
import subprocess

def dbt_transform(dbt_project_dir):
    # Run dbt transformations using subprocess
    try:
        subprocess.run(['dbt', 'run', '--project-dir', dbt_project_dir], check=True)
        print("dbt transformations completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running dbt transformations: {e}")

