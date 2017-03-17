namespace java com.chen.wrpc.test.gen

service MessageService {
    bool sendSMS(1:string mobile, 2:string context);
}