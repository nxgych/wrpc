namespace java com.chen.wrpc.test.gen

struct User {
   1:i64 id,
   2:string name,
   3:string password,
   4:i32 level
}

service UserService {
    string changeName(1:string name);
    User get(1:i64 id);
    bool create(1:User user);
    bool modifyPassword(1:string password);
}