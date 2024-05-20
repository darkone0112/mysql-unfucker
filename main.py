from max_to_min import transfer_rows as transfer_rows_min_to_max
from max_to_min import transfer_rows as transfer_rows_max_to_min

def main():
    choice = input("Choose direction (1 for Min to Max, 2 for Max to Min): ")
    if choice == '1':
        transfer_rows_min_to_max()
    elif choice == '2':
        transfer_rows_max_to_min()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
