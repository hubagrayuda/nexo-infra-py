import math
from nexo.types.float import SeqOfFloats, ManyFloats
from nexo.types.integer import ManyInts


def compute_percentile(values: SeqOfFloats, p: int) -> float:
    """
    Compute percentile using linear interpolation.
    p should be between 0 and 100.
    """
    if not values:
        return 0.0

    if not 0 <= p <= 100:
        raise ValueError("p must be between 0 and 100")

    sorted_values = sorted(values)
    n = len(sorted_values)

    if n == 1:
        return float(sorted_values[0])

    # Position in array (0-based index)
    rank = (p / 100) * (n - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)

    if lower == upper:
        return float(sorted_values[int(rank)])

    weight = rank - lower
    return float(sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight)


def compute_percentiles(
    values: SeqOfFloats,
    percentiles: ManyInts,
) -> ManyFloats:
    if not values:
        return tuple(0.0 for _ in percentiles)

    sorted_values = sorted(values)
    n = len(sorted_values)

    results: list[float] = []

    for p in percentiles:
        rank = (p / 100) * (n - 1)
        lower = math.floor(rank)
        upper = math.ceil(rank)

        if lower == upper:
            results.append(float(sorted_values[int(rank)]))
        else:
            weight = rank - lower
            results.append(
                float(
                    sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
                )
            )

    return tuple(results)
