from terminal import * 


def main(): 

    first_function = sys.argv[1]
    UserName = 'test'
    PassWord = 'password'
    DataBase_Name = 'cs122a' 

    cur, con = connect(UserName, PassWord, DataBase_Name) 
    if first_function == 'import': 
        DB_creation(cur, DataBase_Name)
        parsefiles(cur, con, sys.argv[2])

if __name__ == '__main__':
    main()