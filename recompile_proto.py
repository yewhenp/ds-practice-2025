import os
from pathlib import Path
import subprocess

PROTO_DIR = "utils/pb"

def proto_command(service):
    if service not in os.listdir():
        os.mkdir(service)
    elif not Path(service).is_dir():
        raise RuntimeError(f"{service} is not a directory.")
    subprocess.run(
        ["python", "-m", "grpc_tools.protoc", "-I.", 
         f"--python_out=./{service}", 
         f"--pyi_out=./{service}", 
         f"--grpc_python_out=./{service}",
         f"{service}.proto"]
    )

def get_services():
    file_names = os.listdir(PROTO_DIR)
    return [
        file_name.replace('.proto', '')
        for file_name in file_names
        if file_name.endswith('.proto')
    ]

def run_proto(services):
    os.chdir("utils/pb")
    list(map(proto_command, services))
    os.chdir('../../')

if __name__ == "__main__":
    services = get_services()
    run_proto(services)
    