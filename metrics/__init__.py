from .aggregator import (
    calculate_process_metrics_optimized,
    calculate_process_metrics_stream,
    merge_metrics_results
)

# For backward compatibility
__all__ = [
    'calculate_process_metrics_optimized',
    'calculate_process_metrics_stream',
    'merge_metrics_results'
]