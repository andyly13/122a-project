from terminal import * 


def main(): 
    UserName = 'test'
    PassWord = 'password'
    DataBase_Name = 'cs122a' 

    cur, con = connect(UserName, PassWord, DataBase_Name)  
    if cur is None or con is None:
        print("Failed to connect to the database.")
        return

    DB_creation(cur, DataBase_Name)

if __name__ == "__main__":
    main()