from min_to_max import transfer_rows as transfer_rows_min_to_max
from max_to_min import transfer_rows as transfer_rows_max_to_min
import os

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    table_name = input("Enter the name of the corrupted table: ")
    backup_table_name = f"backup_{table_name}"

    print("Choose an option:")
    print("1. Process from Min to Max")
    print("2. Process from Max to Min")
    print("3. Full Transaction (Min to Max, then Max to Min)")
    choice = input("Enter your choice: ")

    if choice == '1':
        last_successful_min_to_max = transfer_rows_min_to_max(table_name, backup_table_name)
        print(f"Last successful insert from Min to Max: {last_successful_min_to_max}")
    elif choice == '2':
        last_successful_max_to_min = transfer_rows_max_to_min(table_name, backup_table_name)
        print(f"Last successful insert from Max to Min: {last_successful_max_to_min}")
    elif choice == '3':
        clear_screen()
        last_successful_min_to_max = transfer_rows_min_to_max(table_name, backup_table_name)
        last_successful_max_to_min = transfer_rows_max_to_min(table_name, backup_table_name)
        
        total_data_loss = last_successful_max_to_min - last_successful_min_to_max + 1

        print("Transaction Summary:")
        print(f"Last successful insert from Min to Max: {last_successful_min_to_max}")
        print(f"Last successful insert from Max to Min: {last_successful_max_to_min}")
        print(f"Total potential data loss (rows): {total_data_loss}")
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
