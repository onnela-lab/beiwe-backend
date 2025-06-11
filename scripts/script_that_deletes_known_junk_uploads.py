from multiprocessing.pool import ThreadPool
from time import perf_counter

from database.models import S3File
from libs.s3 import s3_delete


"""
Turns out we have a lot of junk uploads, these are files that contain only header lines of uploaded
data, or header line plus a new line.

run S3File.find_duplicates() to find counts for your top 20.
Note that wifi files are excluded because, due to a lack of a timestamp, they are frequently identical.

a791ace733887df6b5d7aa20e7d452b4f2e6c884 - accel - b'timestamp,accuracy,x,y,z'
8a0b1171eff34c1de397f2a737e89148395e0446 - accel - b'timestamp,accuracy,x,y,z\n'
422647432f8e019243f68aa482488eba9f21f203 - bluetoothLog - b'timestamp, hashed MAC, RSSI'
289415afb9270b622385e450b53289a5c514db27 - callLog - b'hashed phone number,call type,timestamp,duration in seconds\n'
3a56cd0133bc2cb707b176cde922b5108892a71a - callLog - b'hashed phone number,call type,timestamp,duration in seconds'
8b20061b68160dedef1e1869f91aae140093cfa2 - gps - b'timestamp,latitude,longitude,altitude,accuracy'
346886aac2f9c19f790ed6213ba07d291a5aa985 - gps - b'timestamp, latitude, longitude, altitude, accuracy'
61946222edd04666c9a0d66d3b30fcfed55aa8fd - gps - b'timestamp, latitude, longitude, altitude, accuracy\n'
4e43222cfe9e42a83ee9b27dd9efd9cfbe935f90 - gyro - b'timestamp,x,y,z'
8eca9ab629212f785a4cf8831feacd41facd7c92 - powerState - b'timestamp, event'
59ef78a196314e0e8f4a01ef8beefb1c5df6355b - powerState - b'timestamp,event,level'
9773475afbaa07fc4beebafc42ea3910bb7a2647 - powerState - b'timestamp, event\n'
9347a7707d92c0866612659696eb92f33a894861 - logFile - b'THIS LINE IS A LOG FILE HEADER\n'
ff77f79cd8d6b46eda2a3511fc6663e97f0db1ea - logFile - b'THIS LINE IS A LOG FILE HEADER'
21f38aedd23cbb507f185303cc914880fb62ec6f - surveyAnswers - b'question id,question type,question text,question answer options,answer\n'
acbd957a28d8833567be73376a20e27bcffa1b19 - surveyAnswers - b'question id,question type,question text,question answer options,answer'
8f111b9d55e88f062bbd1e18f959bad93ea681ba - surveyTimings - b'timestamp,question id,question type,question text,question answer options,answer,event'
3c1d21995db3f94521ca38731bd6a6b1f2b6f193 - textsLog - b'timestamp,hashed phone number,sent vs received,message length,time sent'
deb4da78c8018c2ae7625bf2500912a057123753 - textsLog - b'timestamp,hashed phone number,sent vs received,message length,time sent\n'
96119fae09cdeb6daaf190a049bbac80e95e522e - reachability - b'timestamp,event'


Known correct headers, plus with new lines  (and good, there is overlap with the above)

127b36b8633f7f0407f16705d52f3d76a3f2e5d9 - b'hashed MAC, frequency, RSSI'
0f7f1c13cc73ffe20e83eee2f62749e078377730 - b'hashed MAC, frequency, RSSI\n'
3a56cd0133bc2cb707b176cde922b5108892a71a - b'hashed phone number,call type,timestamp,duration in seconds'
289415afb9270b622385e450b53289a5c514db27 - b'hashed phone number,call type,timestamp,duration in seconds\n'
acbd957a28d8833567be73376a20e27bcffa1b19 - b'question id,question type,question text,question answer options,answer'
21f38aedd23cbb507f185303cc914880fb62ec6f - b'question id,question type,question text,question answer options,answer\n'
ff77f79cd8d6b46eda2a3511fc6663e97f0db1ea - b'THIS LINE IS A LOG FILE HEADER'
9347a7707d92c0866612659696eb92f33a894861 - b'THIS LINE IS A LOG FILE HEADER\n'
8eca9ab629212f785a4cf8831feacd41facd7c92 - b'timestamp, event'
9773475afbaa07fc4beebafc42ea3910bb7a2647 - b'timestamp, event\n'
422647432f8e019243f68aa482488eba9f21f203 - b'timestamp, hashed MAC, RSSI'
f15863796c0d4d6315213e76a49ae415fbb48de0 - b'timestamp, hashed MAC, RSSI\n'
346886aac2f9c19f790ed6213ba07d291a5aa985 - b'timestamp, latitude, longitude, altitude, accuracy'
61946222edd04666c9a0d66d3b30fcfed55aa8fd - b'timestamp, latitude, longitude, altitude, accuracy\n'
a791ace733887df6b5d7aa20e7d452b4f2e6c884 - b'timestamp,accuracy,x,y,z'
8a0b1171eff34c1de397f2a737e89148395e0446 - b'timestamp,accuracy,x,y,z\n'
96119fae09cdeb6daaf190a049bbac80e95e522e - b'timestamp,event'
d4f2302bca18ed9d54b37bb4f08b36688dfbfd1b - b'timestamp,event\n'
59ef78a196314e0e8f4a01ef8beefb1c5df6355b - b'timestamp,event,level'
ade9a92f3f8f3a54deb1641ae9276093088015d9 - b'timestamp,event,level\n'
3c1d21995db3f94521ca38731bd6a6b1f2b6f193 - b'timestamp,hashed phone number,sent vs received,message length,time sent'
deb4da78c8018c2ae7625bf2500912a057123753 - b'timestamp,hashed phone number,sent vs received,message length,time sent\n'
8b20061b68160dedef1e1869f91aae140093cfa2 - b'timestamp,latitude,longitude,altitude,accuracy'
ba6de155b4028f2c542473f1a75b21a54f017a1a - b'timestamp,latitude,longitude,altitude,accuracy\n'
d74c3abd45efefa11ea95bc0987f58ac549a081a - b'timestamp,launchId,memory,battery,event,msg,d1,d2,d3,d4'
574e43f4fd9b8d36b1a70ed5255a70446c6fddb8 - b'timestamp,launchId,memory,battery,event,msg,d1,d2,d3,d4\n'
7dbf867f2f35693d69a4bf0c36ffb21f8e13f81a - b'timestamp,question id,question type,question text,question answer options,answer'
2701ea94a0a8578b480cd69903b958aaaa2ad369 - b'timestamp,question id,question type,question text,question answer options,answer\n'
8f111b9d55e88f062bbd1e18f959bad93ea681ba - b'timestamp,question id,question type,question text,question answer options,answer,event'
3532fbaea0361e2e26f5d5023c95398520fdc33c - b'timestamp,question id,question type,question text,question answer options,answer,event\n'
0ffe6caff593dd99067a61799f6338367b0c374d - b'timestamp,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z'
91a122c1864b62fee435e39da1e6cbacf602c5af - b'timestamp,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z\n'
e6cddda5d6e89f920cf29491f46922ac6a6214a6 - b'timestamp,UTC time,accuracy,x,y,z'
2e73a0f212ad4c8e4871189251e13e50eaf4d420 - b'timestamp,UTC time,accuracy,x,y,z\n'
8aee8136c16597aff010f3c174b8458e2d740233 - b'timestamp,UTC time,event'
072ef7b9505993de878de556551eea87cb331ed4 - b'timestamp,UTC time,event\n'
c0e8fff8e2e00ec90ad0c69e87d6a0803338d699 - b'timestamp,UTC time,event,level'
04945d8ef2bbfaa008a05a1354b2cfa0fcef9312 - b'timestamp,UTC time,event,level\n'
61c8fcd0e352e1850cd184713dccaafebee3156f - b'timestamp,UTC time,hashed MAC,frequency,RSSI'
296ba5b840b1ef1b9e30e57274a06f6f1d878dd2 - b'timestamp,UTC time,hashed MAC,frequency,RSSI\n'
f89842a9391851eb037b97ab0b0cc6076d0f2656 - b'timestamp,UTC time,hashed MAC,RSSI'
c6bf9bb3f8a0ae6a4286df524af97cc582be5a8a - b'timestamp,UTC time,hashed MAC,RSSI\n'
01db54f4d3629ec1df464551489dbd03516dce95 - b'timestamp,UTC time,hashed phone number,call type,duration in seconds'
ce58138419ab256f2b52afa6cc32ea0f95b21232 - b'timestamp,UTC time,hashed phone number,call type,duration in seconds\n'
74e4cf4e6c20665cf55f0497866ed915102b29ed - b'timestamp,UTC time,hashed phone number,sent vs received,message length,time sent'
3c23292f17c1d6185f5a46024afc9619986c5496 - b'timestamp,UTC time,hashed phone number,sent vs received,message length,time sent\n'
1cf33194c4e2d8e951ac86f26c4aaceac9afc996 - b'timestamp,UTC time,latitude,longitude,altitude,accuracy'
03879618e5d85666bdec52b95d4fef77b2b1b8a1 - b'timestamp,UTC time,latitude,longitude,altitude,accuracy\n'
4db27728d1ff6b0bccc6fe1c17be613a78e1d1ca - b'timestamp,UTC time,launchId,memory,battery,event,msg,d1,d2,d3,d4'
a4d9424dc9db68b432a146587ea9b5a7e737f155 - b'timestamp,UTC time,launchId,memory,battery,event,msg,d1,d2,d3,d4\n'
951b31afcb3e17df8782b0b6e84356137f589bfc - b'timestamp,UTC time,patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version'
77c83b34a9bfcd97441d465dd5640510f42d7644 - b'timestamp,UTC time,patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version\n'
d7c0170ea85bb05fb961195259209528e1cb4e8b - b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer'
47f1e17a51a5a4bbf9cadb4fe7829645745b931c - b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer\n'
2cb0183eab9e8bfcd99c4e9ac8ee304cd912eb0c - b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer,event'
a76efa6915a7545e5176a33beb770b3068c603c9 - b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer,event\n'
a4b317e5002e32bf0a6fb805a31ccae90b044a31 - b'timestamp,UTC time,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z'
793e67f098d855479b3020b600739ce0d7bc5b57 - b'timestamp,UTC time,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z\n'
1184ef4c7f8fdffb19a555a596a05cdfa10dae15 - b'timestamp,UTC time,x,y,z'
4992aa194a1477503e07db4b3a34b9c3969337a4 - b'timestamp,UTC time,x,y,z\n'
4e43222cfe9e42a83ee9b27dd9efd9cfbe935f90 - b'timestamp,x,y,z'
49799b5498e73789c094bd5e92e2e606e8df3919 - b'timestamp,x,y,z\n'

the above generated with this, headers taken from data_processing_constants:
x = [
b'hashed MAC, frequency, RSSI',
b'hashed phone number,call type,timestamp,duration in seconds',
b'question id,question type,question text,question answer options,answer',
b'THIS LINE IS A LOG FILE HEADER',
b'timestamp, event',
b'timestamp, hashed MAC, RSSI',
b'timestamp, latitude, longitude, altitude, accuracy',
b'timestamp,accuracy,x,y,z',
b'timestamp,event',
b'timestamp,event,level',
b'timestamp,hashed phone number,sent vs received,message length,time sent',
b'timestamp,latitude,longitude,altitude,accuracy',
b'timestamp,launchId,memory,battery,event,msg,d1,d2,d3,d4',
b'timestamp,question id,question type,question text,question answer options,answer',
b'timestamp,question id,question type,question text,question answer options,answer,event',
b'timestamp,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
b'timestamp,UTC time,accuracy,x,y,z',
b'timestamp,UTC time,event',
b'timestamp,UTC time,event,level',
b'timestamp,UTC time,hashed MAC,frequency,RSSI',
b'timestamp,UTC time,hashed MAC,RSSI',
b'timestamp,UTC time,hashed phone number,call type,duration in seconds',
b'timestamp,UTC time,hashed phone number,sent vs received,message length,time sent',
b'timestamp,UTC time,latitude,longitude,altitude,accuracy',
b'timestamp,UTC time,launchId,memory,battery,event,msg,d1,d2,d3,d4',
b'timestamp,UTC time,patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version',
b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer',
b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer,event',
b'timestamp,UTC time,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
b'timestamp,UTC time,x,y,z',
b'timestamp,x,y,z',
]
for y in x:
    sha = hashlib.sha1(y).digest()
    print(sha.hex(), "-", y)
    y = y + b"\n"
    sha = hashlib.sha1(y).digest()
    print(sha.hex(), "-", y)


"""


sha1_hashes = [
"01db54f4d3629ec1df464551489dbd03516dce95",
"03879618e5d85666bdec52b95d4fef77b2b1b8a1",
"04945d8ef2bbfaa008a05a1354b2cfa0fcef9312",
"072ef7b9505993de878de556551eea87cb331ed4",
"0f7f1c13cc73ffe20e83eee2f62749e078377730",
"0ffe6caff593dd99067a61799f6338367b0c374d",
"1184ef4c7f8fdffb19a555a596a05cdfa10dae15",
"127b36b8633f7f0407f16705d52f3d76a3f2e5d9",
"1cf33194c4e2d8e951ac86f26c4aaceac9afc996",
"21f38aedd23cbb507f185303cc914880fb62ec6f",
"2701ea94a0a8578b480cd69903b958aaaa2ad369",
"289415afb9270b622385e450b53289a5c514db27",
"296ba5b840b1ef1b9e30e57274a06f6f1d878dd2",
"2cb0183eab9e8bfcd99c4e9ac8ee304cd912eb0c",
"2e73a0f212ad4c8e4871189251e13e50eaf4d420",
"346886aac2f9c19f790ed6213ba07d291a5aa985",
"3532fbaea0361e2e26f5d5023c95398520fdc33c",
"3a56cd0133bc2cb707b176cde922b5108892a71a",
"3c1d21995db3f94521ca38731bd6a6b1f2b6f193",
"3c23292f17c1d6185f5a46024afc9619986c5496",
"422647432f8e019243f68aa482488eba9f21f203",
"47f1e17a51a5a4bbf9cadb4fe7829645745b931c",
"49799b5498e73789c094bd5e92e2e606e8df3919",
"4992aa194a1477503e07db4b3a34b9c3969337a4",
"4db27728d1ff6b0bccc6fe1c17be613a78e1d1ca",
"4e43222cfe9e42a83ee9b27dd9efd9cfbe935f90",
"574e43f4fd9b8d36b1a70ed5255a70446c6fddb8",
"59ef78a196314e0e8f4a01ef8beefb1c5df6355b",
"61946222edd04666c9a0d66d3b30fcfed55aa8fd",
"61c8fcd0e352e1850cd184713dccaafebee3156f",
"74e4cf4e6c20665cf55f0497866ed915102b29ed",
"77c83b34a9bfcd97441d465dd5640510f42d7644",
"793e67f098d855479b3020b600739ce0d7bc5b57",
"7dbf867f2f35693d69a4bf0c36ffb21f8e13f81a",
"8a0b1171eff34c1de397f2a737e89148395e0446",
"8aee8136c16597aff010f3c174b8458e2d740233",
"8b20061b68160dedef1e1869f91aae140093cfa2",
"8eca9ab629212f785a4cf8831feacd41facd7c92",
"8f111b9d55e88f062bbd1e18f959bad93ea681ba",
"91a122c1864b62fee435e39da1e6cbacf602c5af",
"9347a7707d92c0866612659696eb92f33a894861",
"951b31afcb3e17df8782b0b6e84356137f589bfc",
"96119fae09cdeb6daaf190a049bbac80e95e522e",
"9773475afbaa07fc4beebafc42ea3910bb7a2647",
"a4b317e5002e32bf0a6fb805a31ccae90b044a31",
"a4d9424dc9db68b432a146587ea9b5a7e737f155",
"a76efa6915a7545e5176a33beb770b3068c603c9",
"a791ace733887df6b5d7aa20e7d452b4f2e6c884",
"acbd957a28d8833567be73376a20e27bcffa1b19",
"ade9a92f3f8f3a54deb1641ae9276093088015d9",
"ba6de155b4028f2c542473f1a75b21a54f017a1a",
"c0e8fff8e2e00ec90ad0c69e87d6a0803338d699",
"c6bf9bb3f8a0ae6a4286df524af97cc582be5a8a",
"ce58138419ab256f2b52afa6cc32ea0f95b21232",
"d4f2302bca18ed9d54b37bb4f08b36688dfbfd1b",
"d74c3abd45efefa11ea95bc0987f58ac549a081a",
"d7c0170ea85bb05fb961195259209528e1cb4e8b",
"deb4da78c8018c2ae7625bf2500912a057123753",
"e6cddda5d6e89f920cf29491f46922ac6a6214a6",
"f15863796c0d4d6315213e76a49ae415fbb48de0",
"f89842a9391851eb037b97ab0b0cc6076d0f2656",
"ff77f79cd8d6b46eda2a3511fc6663e97f0db1ea",
]



def main():
    pool = ThreadPool(processes=20)
    
    t1 = perf_counter()
    
    for a_hex in sha1_hashes:
        i = 0
        print(f"Searching files with hash '{a_hex}'...")
        
        # this returns items almost immediately even if the table is massive
        while paths:= list(S3File.flat("path", sha1=bytes.fromhex(a_hex))[:10_000]):
            
            for _ in pool.imap_unordered(s3_delete, paths):
                i += 1
                if i % 1000 == 0 and i > 0:
                    print(f"deleted {i} files so far...")
            
            S3File.fltr(path__in=paths).delete()
        
        print(f"found and deleted {i} files total. (Current runtime: {perf_counter() - t1:.2f} seconds)")
    
    pool.close()
    pool.terminate()
