# Server environment

Verified 2026-07-31 at approximately 19:20 UTC before project creation.

| Item | Observed value |
| --- | --- |
| Operating system | Debian GNU/Linux 13.6 (trixie), Linux 6.12.94 |
| Architecture | x86_64 |
| CPU | 8 logical CPUs |
| RAM | 15 GiB total, 8.7 GiB available at inspection |
| Swap | 4 GiB total, 3.4 GiB free |
| Project filesystem | ext4 on `/dev/vda4` |
| Disk | 503 GiB total, 404 GiB available (17% used) |
| Python | CPython 3.13.5 at `/usr/bin/python3` |
| Docker | 29.7.1; Compose 5.3.1 |
| systemd | 257; system state reported `degraded` because unrelated units are not all healthy |
| Display timezone | Europe/Berlin (CEST, UTC+02 at inspection) |
| UTC clock | 2026-07-31 19:19 UTC at inspection |
| Clock synchronization | `System clock synchronized: yes`; `systemd-timesyncd` active; RTC in UTC |
| Existing repository | Target workspace contained only sandbox metadata; no usable Git repository |
| Target directory | New project created under the writable workspace at `/root/polymarket/btc_15min_bot/polymarket-btc-backtester` |

Listening TCP ports observed before deployment were 22, 80, 443, and 3100 on public
interfaces, with 3000, 3101, 5432, 5678, 8081, 8082, 8090, 8096, 8099, and 44699
bound to loopback. Port 9108 was free. This project binds health and metrics only to
`127.0.0.1:9108` and does not change firewall rules.

All application and data timestamps are written in UTC nanoseconds. Source timestamps are
preserved separately and are never corrected using the local clock.
