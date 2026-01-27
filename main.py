from datasets import load_dataset
import pandas as pd
import re

def is_health_domain(example):
    field = example.get('metadata.field', '')
    if field and re.search(r'health|medicine', field, re.IGNORECASE):
        return True
    return False

def main():
    print("Loading dataset 'katielink/expertqa'...")
    # Streaming mode is safer for memory, though for small datasets standard load is faster.
    # expertqa is likely not huge, but streaming is robust.
    dataset = load_dataset("katielink/expertqa", split="train", streaming=True)
    
    health_data = []
    
    print("Filtering for health domain data...")
    count = 0
    matched = 0
    
    # Iterate through the dataset
    try:
        for example in dataset:
            count += 1
            if is_health_domain(example):
                health_data.append(example)
                matched += 1
                
            if count % 1000 == 0:
                print(f"Processed {count} records, found {matched} health related...", end='\r')
    except Exception as e:
        print(f"\nStopped or Error during iteration: {e}")

    print(f"\nFinished processing. Total records sub-scanned: {count}. Total health records: {matched}.")
    
    if health_data:
        print("Saving to CSV...")
        df = pd.DataFrame(health_data)
        output_file = "expertqa_health_data.csv"
        # encoding='utf-8-sig' allows Excel to open it correctly with special chars
        df.to_csv(output_file, index=False, encoding='utf-8-sig') 
        print(f"Successfully saved {len(df)} records to {output_file}")
    else:
        print("No health data found.")

if __name__ == "__main__":
    main()
