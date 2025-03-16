from terminal import * 


def main(): 
    
    UserName = 'test'
    PassWord = 'password'
    DataBase_Name = 'cs122a' 

    cur, con = connect(UserName, PassWord, DataBase_Name) 


    startingpoint = sys.argv[1]
    if startingpoint == 'import': 
        DB_creation(cur, DataBase_Name)
        print("DB created")
        parsingfiles(cur, con, sys.argv[2])

    if startingpoint in ('insertViewer', 'insertMovie', 'insertSession'):
        insertions(cur, con, startingpoint, sys.argv)

    if startingpoint in('deleteViewer'):
        deletions(cur, con, startingpoint, sys.argv)

    #does genre have a table???
    if startingpoint in ('addGenre'):
        addGenre(cur, con, sys.argv)
        

if __name__ == "__main__":
    main()