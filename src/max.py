import sys
from services.demo_max_service import DemoMax

if __name__ == "__main__":
    try:
        print("Max file Conversion started")
        process = DemoMax()
        process.procees_conversion()
        print("Max File conversion Ended")
    except Exception as exp:
        print("Excetion")
        raise(exp)