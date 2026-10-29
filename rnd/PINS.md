# Pins

| Item | Version | Source |
|---|---|---|
| JuiceFS stable (production) | v1.4.2 `46af7afa9f819b8c9ddafd475fb828d22fe9dd17` | `juicedata/mount:ce-v1.4.2` `sha256:c56f11aeeb3a50c065b3df957f21f90cff7d7845a4ed0caec7530506fae070d1` |
| JuiceFS upstream main | `1dd8856aff60396cfb09971c7789e20604d02a81` (1.5.0-dev) | `juicedata/mount:ce-nightly` `sha256:007f7b60230a068ca44ad3bb5a52c6c504a765c5f66c2dba13d17315d40016e0` |
| Valkey | 9.1.1 | `valkey/valkey:9.1.1` `sha256:64e361b630ecf18dff7ca4df6a88e6eafc193687eb48cff2c7e0a293ab67d29a` |
| Kvrocks | 2.17.0 `185d4221` | `apache/kvrocks:latest` `sha256:a1e99b3653fb900d5a17b80a9487f74079edc6d5e9b70e3103c01cbfc0d5146a` |

Images pulled from `registry.cn-hangzhou.aliyuncs.com/juicedata/mount` (Docker Hub returned 429); digests are the mirror's repo digests.

Stand: cloud container, 4 vCPU, 15 GiB RAM, ~26 GiB free disk, Valkey with `appendonly yes`, `appendfsync always`, `noeviction`, no replicas.
