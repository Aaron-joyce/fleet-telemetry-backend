import os
import signal
import sys
from multiprocessing import Process

from parser import load_events_by_robot
from publisher import run_publisher_sync


def deployFleet() -> None:
    print("[FLEET ORCHESTRATOR] Starting robot telemetry simulation")
    fleet = load_events_by_robot()
    processes:list[Process] = []

    def terminate_fleet(signum, frame):
        print(f"\n[FLEET ORCHESTRATOR] Received signal {signum}. Shutting Down worker processes")
        for p in processes:
            if p.is_alive():
                p.terminate()
        for p in processes:
            p.join(timeout=2.0)
        print("[FLEET ORCHESTRATOR] All robot telemetry simlations stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, terminate_fleet)
    signal.signal(signal.SIGTERM, terminate_fleet)

    print(f"[FLEET ORCHESTRATOR] Spawning {len(fleet)} publisher processes...")
    for robot_id, events in fleet.items():
        p = Process(target=run_publisher_sync, args=(robot_id, events), name=f"Publisher-{robot_id}")
        p.start()
        processes.append(p)
        print(f"\t-> Spawned {p.name} (PID: {p.pid}")

    print("[FLEET RUNNER] All robots publishing messages. Press Ctrl + C to abort")

    for p in processes:
        p.join()

    print("[FLEET ORCHESTRATOR] All Simulation completed.")

if __name__ == "__main__":
    deployFleet()
