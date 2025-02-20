import subprocess
import sys

def install_packages():
    try:
        # Read the requirements.txt file
        with open('requirements.txt', 'r') as file:
            packages = file.read().splitlines()

        # Install each package using pip
        for package in packages:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}")
    except FileNotFoundError:
        print("Error: requirements.txt file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    install_packages()