# Demo interativo do `science_catalogs`

Esta pasta serve para testar a API beta do pacote com dados ficticios.

## O que existe aqui

- `data/input/*.parquet`: dois arquivos parquet pequenos com fotometria falsa
- `configs/demo_parquet.yml`: fluxo para escrever parquet em disco
- `configs/demo_hats.yml`: fluxo para escrever HATS e reabrir com LSDB
- `demo_all.py`: smoke test completo da API beta

O exemplo usa somente transformacao de `flux -> magnitude`, filtro booleano e `initial_cut`.
Nao ativa `dustmaps`, entao nao precisa baixar mapas para este teste.

## Shell interativo

Ative o ambiente e entre no repositorio:

```bash
micromamba activate science_catalogs
cd /home/singulani/projects/science_catalogs
python
```

Dentro do Python:

```python
from pathlib import Path
from science_catalogs import (
    build_catalog,
    materialize_catalog,
    materialize_lsdb_catalog,
    open_lsdb_catalog,
    prepare_catalog,
    write_catalog,
)

root = Path("examples/demo_catalog")
parquet_cfg = root / "configs" / "demo_parquet.yml"
hats_cfg = root / "configs" / "demo_hats.yml"

prepared = prepare_catalog(str(parquet_cfg))
prepared.suffix
prepared.input_files
prepared.ddf.head()

frame = materialize_catalog(prepared)
frame[["object_id", "tile", "mag_g", "mag_r"]]

parquet_out = root / "output" / "manual_parquet"
written_parquet = write_catalog(prepared, str(parquet_out))
written_parquet

prepared_hats = prepare_catalog(str(hats_cfg))
hats_out = root / "output" / "manual_hats"
catalog = materialize_lsdb_catalog(prepared_hats, str(hats_out))
catalog

open_lsdb_catalog(hats_out / "demo_hats_catalog")

build_catalog(str(parquet_cfg), output_mode="memory").data.head()
build_catalog(
    str(parquet_cfg),
    output_mode="disk",
    output_dir=str(root / "output" / "build_parquet"),
).written_paths
build_catalog(
    str(hats_cfg),
    output_mode="lsdb",
    output_dir=str(root / "output" / "build_hats"),
).catalog
```

## Rodar tudo de uma vez

```bash
python examples/demo_catalog/demo_all.py
```
