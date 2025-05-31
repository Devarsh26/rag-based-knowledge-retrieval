#!/usr/bin/env python3
import subprocess
import sys

COMPOSE_FILE = "docker-compose.yml"

def run(cmd):
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        sys.exit("✗ Failed: " + " ".join(cmd))

def containers_exist():
    # returns True if compose has any containers (even stopped ones)
    res = subprocess.run(
        ["docker-compose", "-f", COMPOSE_FILE, "ps", "-q"],
        capture_output=True, text=True
    )
    return bool(res.stdout.strip())

def create_and_start():
    print("First time setup: creating & starting containers")
    run(["docker-compose", "-f", COMPOSE_FILE, "up", "-d"])

def start():
    if not containers_exist():
        create_and_start()
    else:
        print("Starting existing containers")
        run(["docker-compose", "-f", COMPOSE_FILE, "start"])

def stop():
    print("Stopping containers (they remain on disk)")
    run(["docker-compose", "-f", COMPOSE_FILE, "stop"])

def status():
    run(["docker-compose", "-f", COMPOSE_FILE, "ps"])

def usage():
    print("Usage: python StartKafkaContainer.py [start|stop|status]")
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()

    cmd = sys.argv[1].lower()
    if cmd == "start":
        start()
    elif cmd == "stop":
        stop()
    elif cmd == "status":
        status()
    else:
        usage()
