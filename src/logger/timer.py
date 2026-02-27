# start and end a timer with a message
import time

# time should save in local file
def set_timer(message: str) -> float:
    current_time = time.time()
    return current_time

def calculate_time(start_time: float, end_time:float) -> float:
    return end_time - start_time