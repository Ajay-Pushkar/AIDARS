import time
from typing import List, Callable, Any
from dataclasses import dataclass
import statistics
import logging

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    name: str
    count: int
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    avg_ms: float
    
    def print_report(self):
        print(f"\n[{self.name}] N={self.count}")
        print(f"  Min: {self.min_ms:.2f} ms")
        print(f"  p50: {self.p50_ms:.2f} ms")
        print(f"  Avg: {self.avg_ms:.2f} ms")
        print(f"  p95: {self.p95_ms:.2f} ms")
        print(f"  p99: {self.p99_ms:.2f} ms")
        print(f"  Max: {self.max_ms:.2f} ms")


class BenchmarkRunner:
    def __init__(self, name: str, iterations: int = 10, warmup: int = 2):
        self.name = name
        self.iterations = iterations
        self.warmup = warmup
        self.times: List[float] = []

    def record_ms(self, duration_ms: float):
        self.times.append(duration_ms)

    def calculate(self) -> BenchmarkResult:
        if not self.times:
            raise ValueError("No benchmark data recorded")
        
        sorted_times = sorted(self.times)
        n = len(sorted_times)
        
        def p(percentile: float) -> float:
            k = (n - 1) * percentile
            f = int(k)
            c = f + 1
            if f == c:
                return sorted_times[f]
            d0 = sorted_times[f] * (c - k)
            d1 = sorted_times[c] * (k - f)
            return d0 + d1

        return BenchmarkResult(
            name=self.name,
            count=n,
            min_ms=sorted_times[0],
            p50_ms=p(0.50),
            p95_ms=p(0.95),
            p99_ms=p(0.99),
            max_ms=sorted_times[-1],
            avg_ms=statistics.mean(sorted_times)
        )
