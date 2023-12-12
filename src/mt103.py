import sys
from services.demo_mt103_service import Demo103

if __name__ == "__main__":
    try:
        print("MT103 file Conversion started")
        process = Demo103()
        process.procees_conversion()
        print("MT103 File conversion Ended")
    except Exception as exp:
        print("Excetion")
        raise(exp)