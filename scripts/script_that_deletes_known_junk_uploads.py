from database.models import S3File

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
"""

hexen = [
    "a791ace733887df6b5d7aa20e7d452b4f2e6c884",
    "8a0b1171eff34c1de397f2a737e89148395e0446",
    "422647432f8e019243f68aa482488eba9f21f203",
    "289415afb9270b622385e450b53289a5c514db27",
    "3a56cd0133bc2cb707b176cde922b5108892a71a",
    "8b20061b68160dedef1e1869f91aae140093cfa2",
    "346886aac2f9c19f790ed6213ba07d291a5aa985",
    "61946222edd04666c9a0d66d3b30fcfed55aa8fd",
    "8eca9ab629212f785a4cf8831feacd41facd7c92",
    "59ef78a196314e0e8f4a01ef8beefb1c5df6355b",
    "9773475afbaa07fc4beebafc42ea3910bb7a2647",
    "9347a7707d92c0866612659696eb92f33a894861",
    "ff77f79cd8d6b46eda2a3511fc6663e97f0db1ea",
    "21f38aedd23cbb507f185303cc914880fb62ec6f",
    "acbd957a28d8833567be73376a20e27bcffa1b19",
    "8f111b9d55e88f062bbd1e18f959bad93ea681ba",
    "3c1d21995db3f94521ca38731bd6a6b1f2b6f193",
    "deb4da78c8018c2ae7625bf2500912a057123753",
    "96119fae09cdeb6daaf190a049bbac80e95e522e",
]

def main():
    for hex in hexen:
        sfile: S3File
        for sfile in S3File.vlist(sha1=bytes.fromhex(hex)):
            storage = sfile.storage()
            storage._s3_delete_zst()