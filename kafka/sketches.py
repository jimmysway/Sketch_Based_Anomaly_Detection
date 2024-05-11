import random
import hashlib
import numpy as np
import math

class Sketch:
    def update(self, x):
        raise NotImplementedError("Must be implemented by the subclass.")

    def estimate(self):
        raise NotImplementedError("Must be implemented by the subclass.")

class RobustSketch(Sketch):
    """Adversarial Robustness via Algorithm Switching. If A is a non-robust algorithm that provides (1 + epsilon/20) multiplicative approximation.

    Args:
        Sketch (object): Abstract sketch class
    """
    def __init__(self, num_copies, epsilon, m):
        self.num_copies = int(math.ceil(1 / epsilon * math.log(m)))
        self.sketches = [self.create_sketch() for _ in range(self.num_copies)]
        self.epsilon = epsilon
        self.current_estimate = 0
        self.current_index = 0

    def create_sketch(self):
        raise NotImplementedError("Must be implemented by the subclass to create a sketch instance.")

    def update(self, x):
        for sketch in self.sketches:
            sketch.update(x)

    def estimate(self):
        new_estimate = self.sketches[self.current_index].estimate()
        # Check if the new estimate is significantly larger than the current estimate.
        # The threshold for being 'significantly larger' is defined by epsilon.
        if new_estimate >= (1 + self.epsilon / 2) * self.current_estimate:
            # If the new estimate is larger, update the current estimate to this new value
            self.current_estimate = new_estimate
            
            # Move to the next sketch in the list. Wrap around to the beginning if at the end of the list
            self.current_index = (self.current_index + 1) % self.num_copies
        
        # Return the most recent estimate
        return self.current_estimate

    

class CountMinSketch:
    def __init__(self, width, depth):
        self.width = width
        self.depth = depth
        self.table = np.zeros((depth, width))
        self.seed = list(range(depth))  # Different seed for each hash function

    def _hash(self, x, seed):
        # Using md5 hash function from hashlib and returning a hash value
        h = hashlib.md5()
        h.update(bytes(f"{x}-{seed}", 'utf-8'))
        return int(h.hexdigest(), 16)

    def add(self, x):
        for i in range(self.depth):
            index = self._hash(x, self.seed[i]) % self.width
            self.table[i, index] += 1

    def count(self, x):
        min_count = float('inf')
        for i in range(self.depth):
            index = self._hash(x, self.seed[i]) % self.width
            min_count = min(min_count, self.table[i, index])
        return min_count
    
class RobustCountMinSketch(CountMinSketch):
    def __init__(self, width, depth, epsilon, m):
        super().__init__(width, depth)
        self.epsilon = epsilon
        self.m = m
        self.num_copies = int(math.ceil(1/epsilon * math.log(m)))
        self.copies = [CountMinSketch(width, depth) for _ in range(self.num_copies)]
        self.estimate = 0
        self.current_index = 0

    def add(self, x):
        for cms in self.copies:
            cms.add(x)

    def count(self, x):
        estimates = [cms.count(x) for cms in self.copies]
        new_estimate = estimates[self.current_index]
        if new_estimate >= (1 + self.epsilon / 2) * self.estimate:
            self.estimate = new_estimate
            self.current_index = (self.current_index + 1) % self.num_copies
        return self.estimate
    
class RobustAMSSketch(RobustSketch):
    def __init__(self, num_samples, epsilon, m):
        self.num_samples = num_samples
        super().__init__(num_samples, epsilon, m)

    def create_sketch(self):
        return AMSSketch(self.num_samples)

class AMSSketch(Sketch):
    def __init__(self, num_samples):
        self.num_samples = num_samples
        self.samples = [0] * num_samples
        self.hash_seeds = [random.randint(1, 10000) for _ in range(num_samples)]

    def _hash(self, x, seed):
        h = hashlib.sha256()
        h.update(bytes(f"{x}-{seed}", 'utf-8'))
        hash_value = int(h.hexdigest(), 16)
        return -1 if hash_value % 2 == 0 else 1

    def update(self, x):
        for i in range(self.num_samples):
            projection = self._hash(x, self.hash_seeds[i])
            self.samples[i] += projection

    def estimate(self):
        variance = sum(sample ** 2 for sample in self.samples) / self.num_samples
        return variance


class HyperLogLog(Sketch):
    def __init__(self, b=10):
        self.b = b
        self.m = 1 << b
        self.registers = [0] * self.m
        self.alpha_m = self.get_alpha_m(self.m)

    def get_alpha_m(self, m):
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return 0.7213 / (1 + 1.079 / m)

    def update(self, item):
        hash_value = hashlib.sha256(str(item).encode()).hexdigest()
        hash_int = int(hash_value, 16)
        index = hash_int & (self.m - 1)
        w = hash_int >> self.b
        rank = self._rank(w)
        if rank > self.registers[index]:
            self.registers[index] = rank

    def _rank(self, hash_value):
        return len(bin(hash_value)) - bin(hash_value).find('1')

    def estimate(self):
        indicator = sum([2 ** -r for r in self.registers])
        raw_estimate = self.alpha_m * self.m ** 2 / indicator
        if raw_estimate <= (5/2) * self.m:
            v = self.registers.count(0)
            if v > 0:
                return self.m * math.log(self.m / v)
            else:
                return raw_estimate
        else:
            return raw_estimate

class RobustHyperLogLog(RobustSketch):
    def __init__(self, b, epsilon, m):
        self.b = b
        super().__init__(int(math.ceil(1 / epsilon * math.log(m))), epsilon, m)

    def create_sketch(self):
        return HyperLogLog(self.b)
    

# Example usage
if __name__ == "__main__":
    robust_ams = RobustAMSSketch(100, 0.1, 1000)  # 100 samples, epsilon=0.1, m=1000
    data_points = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
    print(type(robust_ams))
    
    for point in data_points:
        robust_ams.update(point)
    
    estimated_variance = robust_ams.estimate()
    print("Robust estimated variance (F2) of the stream:", estimated_variance)

    cms = RobustCountMinSketch(1000, 10, 0.1, 100)  # Width=1000, Depth=10
    data_points = ["apple", "banana", "orange", "apple", "banana", "banana"]

    for point in data_points:
        cms.add(point)

    print("Frequency of 'apple':", cms.count("apple"))
    print("Frequency of 'banana':", cms.count("banana"))
    print("Frequency of 'orange':", cms.count("orange"))

    hll = RobustHyperLogLog(10, 0.1, 100)  # 2^10 buckets
    data_points = [f"user_{i}" for i in range(1000)]
    for point in data_points:
        hll.update(point)

    print("Estimated number of distinct elements:", hll.estimate())

