# MIDAS Fetcher Cache Structure

## New Cache Directory Structure

With the updated cache system, files are now organized as follows:

### Data Files (with quality control versions)
```
cache/
└── uk-daily-temperature-obs/
    ├── qcv-0/
    │   └── devon/
    │       └── 01381_twist/
    │           ├── midas-open_..._qcv-0_1980.csv
    │           ├── midas-open_..._qcv-0_1981.csv
    │           └── ...
    └── qcv-1/
        └── devon/
            └── 01381_twist/
                ├── midas-open_..._qcv-1_1980.csv
                ├── midas-open_..._qcv-1_1981.csv
                └── ...
```

### Capability and Metadata Files (NEW: separate folder)
```
cache/
└── uk-daily-temperature-obs/
    └── capability/
        └── devon/
            └── 01381_twist/
                ├── midas-open_..._capability.csv
                └── midas-open_..._metadata.csv
```

## Benefits

1. **Clean Separation**: Capability and metadata files are no longer mixed with data files
2. **No "no-quality" directories**: Capability files have their own dedicated space
3. **Consistent Structure**: Both data and capability files follow county/station organization
4. **Future-proof**: Easy to extend for additional file types

## Implementation

- **Data files**: Use quality version directories (`qcv-0`, `qcv-1`, etc.)
- **Capability files**: Use dedicated `capability` directory 
- **Metadata files**: Also placed in `capability` directory
- **Fallback**: Files without type info still use quality version or `no-quality`

This change was implemented in `src/app/cache.rs` in the `get_file_path` function.