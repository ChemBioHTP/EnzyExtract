persistent_input = None
def get_user_y_n():
    global persistent_input
    if persistent_input is True:
        inp = 'y'
    elif persistent_input is False:
        inp = 'n'
    else:
        inp = input("Proceed? (y/n): ")
    if inp == 'Y':
        persistent_input = True
    elif inp == 'N':
        persistent_input = False
    return inp