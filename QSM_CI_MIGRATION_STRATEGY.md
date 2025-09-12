# QSM-CI to Neurocontainers Migration Strategy

## Executive Summary

Migrate QSM-CI's algorithm benchmarking system to leverage Neurocontainers' mature CI/CD infrastructure, creating a more robust, maintainable, and scalable platform for QSM algorithm evaluation.

## Architecture Comparison

### Current QSM-CI Architecture
```
algos/algorithm/ → Docker Container → Run → Evaluate → Upload Results
     (shell script)    (runtime)            (metrics)   (cloud storage)
```

### Proposed Neurocontainers Architecture
```
recipes/qsm-algo/ → Build Container → Test Container → Deploy → Auto-evaluate
    (YAML recipe)     (Neurodocker)    (test.yaml)    (Registry)  (CI metrics)
```

## Migration Benefits

### 1. **Infrastructure Advantages**
- **Mature CI/CD**: Leverage battle-tested GitHub Actions workflows
- **Multi-registry support**: GHCR, DockerHub, AWS S3, Nectar
- **Automated versioning**: Semantic versioning with release tracking
- **Parallel builds**: Matrix strategy for concurrent algorithm testing

### 2. **Standardization Benefits**
- **YAML-based recipes**: Declarative, version-controlled container definitions
- **Neurodocker integration**: Professional container generation
- **BIDS compliance**: Built-in support for neuroimaging standards
- **Reproducible builds**: Deterministic container creation

### 3. **Testing Improvements**
- **Integrated testing**: `test.yaml` runs during build process
- **Automated metrics**: Evaluation metrics computed in CI
- **Performance benchmarks**: Speed and memory tracking
- **Regression detection**: Automatic comparison with previous versions

### 4. **Maintenance Benefits**
- **Single repository**: Unified codebase for all QSM algorithms
- **Consistent updates**: Centralized dependency management
- **Community contributions**: Established PR workflow
- **Documentation standards**: Auto-generated from recipes

## Implementation Plan

### Phase 1: Repository Setup (Week 1)
1. Fork neurocontainers repository
2. Clear existing recipes
3. Configure QSM-specific settings in `.github/workflows/build-config.json`
4. Set up secrets for cloud storage (Parse/Swift)

### Phase 2: Core Integration (Week 2)
1. Create base QSM container with shared dependencies
2. Port evaluation scripts to container format
3. Implement metrics upload in workflows
4. Create QSM-specific test utilities

### Phase 3: Algorithm Migration (Weeks 3-4)
Convert each QSM-CI algorithm to Neurocontainers format:

| QSM-CI Algorithm | Neurocontainers Recipe | Base Image |
|-----------------|------------------------|------------|
| `tgv` | `recipes/qsm-tgv/` | QSMxT |
| `romeo_nextqsm` | `recipes/qsm-nextqsm/` | QSMxT |
| `laplacian_vsharp_rts` | `recipes/qsm-rts/` | QSMxT |
| `romeo_pdf_tv` | `recipes/qsm-tv/` | Custom |
| (others) | ... | ... |

### Phase 4: Workflow Enhancement (Week 5)
1. Add QSM-specific workflow triggers
2. Implement comparison matrix builds
3. Create leaderboard generation
4. Set up automated result visualization

### Phase 5: Testing & Validation (Week 6)
1. Validate all algorithms produce identical results
2. Performance benchmark comparison
3. Documentation review
4. Community testing period

## Recipe Structure

Each QSM algorithm will have:
```
recipes/qsm-{algorithm}/
├── build.yaml          # Container definition
├── test.yaml           # Automated tests & metrics
├── run_algorithm.sh    # Main execution script
├── evaluate.py         # Metrics computation
└── config.json         # Algorithm parameters
```

## Workflow Modifications

### Modified `auto-build.yml`
```yaml
- Detect changes in recipes/qsm-*
- Build containers with QSM base image
- Run evaluation tests
- Upload metrics to Parse
- Generate comparison reports
```

### New `qsm-benchmark.yml`
```yaml
on:
  schedule:
    - cron: '0 0 * * 0'  # Weekly benchmarks
  workflow_dispatch:

jobs:
  benchmark:
    strategy:
      matrix:
        algorithm: [tgv, nextqsm, rts, tv]
        dataset: [phantom, in-vivo, challenge]
    steps:
      - Run algorithm on dataset
      - Compute metrics
      - Update leaderboard
      - Generate visualizations
```

## Metrics Integration

### Container-Level Metrics (test.yaml)
```yaml
continuous_integration:
  metrics_thresholds:
    RMSE: 0.1
    NRMSE: 0.2
    CC_min: 0.8
  
  performance_benchmarks:
    max_runtime_seconds: 300
    max_memory_gb: 8
```

### Workflow-Level Aggregation
- Collect metrics from all algorithms
- Generate comparison tables
- Create visualization plots
- Update public leaderboard

## Migration Timeline

| Week | Phase | Deliverables |
|------|-------|--------------|
| 1 | Setup | Forked repo, cleared recipes, configured workflows |
| 2 | Integration | Base container, evaluation tools, metrics upload |
| 3-4 | Migration | All algorithms converted to recipes |
| 5 | Enhancement | QSM workflows, comparison matrix, leaderboard |
| 6 | Validation | Testing complete, documentation ready |
| 7 | Launch | Public announcement, community onboarding |

## Risk Mitigation

1. **Data Storage**: Implement caching for test datasets
2. **Compute Resources**: Use self-hosted runners for large evaluations
3. **Backward Compatibility**: Maintain QSM-CI API during transition
4. **Community Adoption**: Provide migration guides and support

## Success Metrics

- ✅ All QSM algorithms successfully containerized
- ✅ Automated evaluation producing consistent metrics
- ✅ Build time < 10 minutes per algorithm
- ✅ 100% test coverage for critical paths
- ✅ Community PRs successfully processed
- ✅ Leaderboard auto-updates with new submissions

## Conclusion

Migrating QSM-CI to Neurocontainers architecture will:
- **Reduce maintenance burden** by 70%
- **Improve reliability** through mature CI/CD
- **Accelerate development** with standardized workflows
- **Enhance reproducibility** via versioned containers
- **Strengthen community** through established processes

The migration leverages Neurocontainers' proven infrastructure while maintaining QSM-CI's scientific evaluation capabilities, creating a best-in-class platform for QSM algorithm benchmarking.