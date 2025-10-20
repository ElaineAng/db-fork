import numpy


# ================= Distribution Functions =================
#   Functions that generates a distribution for sampling
# ==========================================================
def beta_distribution(sample_size, alpha, beta):
    """
    Generate skewed values using Beta distribution (output is [0, 1])
    """
    skew_type = "uniform"
    if alpha < beta:
        skew_type = f"right-skewed (a={alpha}, b={beta})"
    elif alpha > beta:
        skew_type = f"left-skewed (a={alpha}, b={beta})"

    print(f"Using {skew_type} distribution...")

    return numpy.random.beta(a=alpha, b=beta, size=sample_size)


# =================== Sampling Utils =======================
#   Utility functions for sampling operations
# ==========================================================
def sort_population(population: list[tuple], idx: int) -> None:
    """
    Sorts the population based on the idx column in the population.

    Args:
        population: The original population list.
        idx: The index of the element in the tuple to sort by.
    """
    population.sort(key=lambda x: x[idx])


def get_sampled_indices(
    population_size, sampling_rate, max_sampling_size, dist_lambda
):
    """
    Generates skewed sample indices based on the provided distribution function.

    Args:
        population_size: The size of the population to sample from.
        sampling_size: Number of samples to generate.
        dist_func: The distribution function to use (e.g., numpy.random.beta).
        **kwargs: Additional parameters for the distribution function.

    Returns:
        List of sampled indices.
    """
    if not (0.0 < sampling_rate <= 1.0):
        raise ValueError("Sampling rate must be between 0.0 and 1.0.")

    # Create the sampling pool based on the sampling rate.
    sampling_size = max(
        1, min(max_sampling_size, int(population_size * sampling_rate))
    )

    print(
        f"Sampling {sampling_size} out of {population_size} total items "
        f"(sampling rate: {sampling_rate:.2f})..."
    )

    # Generate skewed values using the specified distribution function
    raw_indices = dist_lambda(sampling_size)

    # Scale values to become valid integer indices for the population size
    scaled_indices = (raw_indices * (population_size - 1)).astype(int)

    return scaled_indices.tolist()
