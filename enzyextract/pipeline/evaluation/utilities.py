from colorama import Fore, Style


def printred(label, value):
    """
    Print a label and value in blue color.
    """
    print(f"{label}: {Fore.RED}{value}{Style.RESET_ALL}")

def printg(label, value):
    """
    Print a label and value in blue color.
    """
    print(f"{label}: {Fore.GREEN}{value}{Style.RESET_ALL}")

def printb(heading):
    """
    Print a heading  in blue color.
    """
    print(f"{Fore.BLUE}{heading}{Style.RESET_ALL}")