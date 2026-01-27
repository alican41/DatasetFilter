from datasets import load_dataset
import pandas as pd

try:
    print("Loading dataset...")
    # Use streaming to check categories
    dataset = load_dataset("katielink/expertqa", split="train", streaming=True)
    
    fields = set()
    count = 0
    max_check = 2000
    
    print(f"Checking first {max_check} examples for fields...")
    for example in dataset:
        fields.add(example['metadata.field'])
        count += 1
        if count >= max_check:
            break
            
    print("\nUnique fields found:")
    for f in sorted(list(fields)):
        print(f)

except Exception as e:
    print(f"An error occurred: {e}")
