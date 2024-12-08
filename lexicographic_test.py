from geohash import GeoHash

def run_benchmark():
    geohash = 'u1kwus'
    test_cases = [
        ('u1kwum', 'u1kwuu'),
        ('u1kws', 'u1kwv'),
        ('u1kws', 'u1kww'),
        ('u1', 'u2')
    ]

    for _ in range(1_000_000):
        for min_hash, max_hash in test_cases:
            GeoHash.check_within_bounds_lexicographic(geohash, min_hash, max_hash)

if __name__ == "__main__":
    run_benchmark()
