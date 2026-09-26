"""One-shot initialisation of the shared ``<home>/config`` volume (docker compose, 01 §7.2 D2).

compose runs this as the ``config-init`` service before any other container
(``depends_on: condition: service_completed_successfully``), so the services never race
to create the first config version on the shared volume.

* First run (empty volume): the image's bundled defaults are staged with the docker
  profile applied, then ``ensure_home_config`` copies them to ``<home>/config`` and
  commits them as version 1 (``config/versions/1`` + ``config/active.json``). The docker
  profile is:
    - ``services.yaml`` replaced by the compose overlay mounted next to this script
      (every endpoint on its compose service name);
    - ``sim.yaml`` / ``feeds.yaml``: ``127.0.0.1`` becomes ``sim-exchange`` - in these two
      files it only ever names sim-exchange's listeners (Feed A/B, broker, control) and the
      Feed A/B URLs md-gateway connects to.
* Later runs leave the volume alone (admins change the configuration through
  ``POST /api/v1/config/reload``; the api-gateway writes new versions there) and only
  validate the active version.

Exit code 0 when the active version is valid, 1 otherwise.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from futrisk.common.config import ConfigError, ensure_home_config, load_active_config
from futrisk.common.paths import HomeLayout, bundled_config_dir, resolve_home

OVERLAY = Path(__file__).with_name("services.yaml")
SIM_HOST = "sim-exchange"
SIM_FILES = ("sim.yaml", "feeds.yaml")


def stage_docker_profile(dest: Path) -> None:
    """Bundled defaults + the docker profile, written to ``dest``."""
    shutil.copytree(bundled_config_dir(), dest)
    shutil.copy2(OVERLAY, dest / "services.yaml")
    for name in SIM_FILES:
        path = dest / name
        path.write_text(path.read_text(encoding="utf-8").replace("127.0.0.1", SIM_HOST), encoding="utf-8")


def main() -> int:
    layout = HomeLayout(resolve_home())
    fresh = not (layout.config_dir / "app.yaml").exists()
    try:
        if fresh:
            with tempfile.TemporaryDirectory() as tmp:
                staged = Path(tmp) / "config"
                stage_docker_profile(staged)
                ensure_home_config(layout, bundled=staged)
        else:
            ensure_home_config(layout)  # a no-op once active.json exists
        cfg = load_active_config(layout)
    except (ConfigError, OSError) as exc:
        print(f"config-init: {exc}", file=sys.stderr)
        return 1
    action = "initialised" if fresh else "kept"
    print(
        f"config-init: {action} {layout.config_dir}: active version {cfg.config_version} "
        f"(hash {cfg.config_hash[:12]}), profile {cfg.services.profile}, mode {cfg.app.mode}, "
        f"{len(cfg.services.instances)} bus instances"
    )
    for name, inst in sorted(cfg.services.instances.items()):
        print(f"  {name:16} pub {inst.pub}  router {inst.router}" + (f"  http {inst.http}" if inst.http else ""))
    sim = cfg.sim
    print(f"  sim-exchange     feed A {sim.feed_a.host}:{sim.feed_a.port}  feed B {sim.feed_b.host}:{sim.feed_b.port}  "
          f"broker {sim.broker.endpoint}  control {sim.control.host}:{sim.control.port}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
