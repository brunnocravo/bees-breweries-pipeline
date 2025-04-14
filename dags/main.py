from datetime import datetime
from extract_task import extract_all_breweries
from silver_task import transform_to_silver
from gold_task import transform_to_gold

if __name__ == "__main__":
    execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    extract_all_breweries(execution_time=execution_time)
    transform_to_silver(execution_time=execution_time)
    transform_to_gold(execution_time=execution_time)
