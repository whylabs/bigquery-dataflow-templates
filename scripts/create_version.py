import sys

if __name__ == "__main__":
    # Get first arg
    version_arg = sys.argv[1]

    print(
        f"""
# Auto generated file
def get_version() -> str:
    return "{version_arg}"
    """
    )
