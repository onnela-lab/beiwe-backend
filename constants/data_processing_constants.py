## Chunks
# This value is in seconds, it sets the time period that chunked files will be sliced into.
from constants.data_stream_constants import (ACCELEROMETER, ANDROID_LOG_FILE, BLUETOOTH, OMNIRING, CALL_LOG,
                                             DEVICE_IDENTIFIERS_HEADER, DEVICEMOTION, GPS, GYRO, IDENTIFIERS,
                                             IOS_LOG_FILE, MAGNETOMETER,
                                             POWER_STATE, PROXIMITY, REACHABILITY, SURVEY_TIMINGS, TEXTS_LOG, WIFI)
from constants.user_constants import ANDROID_API, IOS_API

CHUNK_TIMESLICE_QUANTUM = 3600

# These reference dicts contain the output headers that should exist for each data stream, per-os.
#  A value of None means that the os cannot generate that data (or the dictionary needs to be updated)

# 2023-2-1: now that we allow participants to change os mid-study, the ios and android specific
#  datastreams would show up in data processing with "missing header" issues due to the prior
#  existence of Nones in this data structure.  I duplicated the strings and have annotated the ones
#  that are "wrong".

REFERENCE_CHUNKREGISTRY_HEADERS = {
    ACCELEROMETER: {
        ANDROID_API: b'timestamp,UTC time,accuracy,x,y,z',
        IOS_API: b'timestamp,UTC time,accuracy,x,y,z',
    },
    ANDROID_LOG_FILE: {
        ANDROID_API: b'timestamp,UTC time,event',
        IOS_API: b'timestamp,UTC time,event',
    },
    BLUETOOTH: {
        ANDROID_API: b'timestamp,UTC time,hashed MAC,RSSI',
        IOS_API: b'timestamp,UTC time,hashed MAC,RSSI',  # android-only data stream
    },
    OMNIRING: {
        ANDROID_API: b'timestamp,UTC time,PPG_red,PPG_IR,PPG_Green,IMU_Accel_x,IMU_Accel_y,IMU_Accel_z,IMU_Gyro_x,IMU_Gyro_y,IMU_Gyro_z,IMU_Mag_x,IMU_Mag_y,IMU_Mag_z,temperature,timestamp',
        IOS_API: b'timestamp,UTC time,PPG_red,PPG_IR,PPG_Green,IMU_Accel_x,IMU_Accel_y,IMU_Accel_z,IMU_Gyro_x,IMU_Gyro_y,IMU_Gyro_z,IMU_Mag_x,IMU_Mag_y,IMU_Mag_z,temperature,timestamp',
    },
    CALL_LOG: {
        ANDROID_API: b'timestamp,UTC time,hashed phone number,call type,duration in seconds',
        IOS_API: b'timestamp,UTC time,hashed phone number,call type,duration in seconds',  # android-only data stream
    },
    DEVICEMOTION: {
        ANDROID_API: b'timestamp,UTC time,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
        # ios-only data stream
        IOS_API: b'timestamp,UTC time,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
    },
    GPS: {
        ANDROID_API: b'timestamp,UTC time,latitude,longitude,altitude,accuracy',
        IOS_API: b'timestamp,UTC time,latitude,longitude,altitude,accuracy',
    },
    GYRO: {
        ANDROID_API: b'timestamp,UTC time,accuracy,x,y,z',
        IOS_API: b'timestamp,UTC time,x,y,z',
    },
    IDENTIFIERS: {
        ANDROID_API: b'timestamp,UTC time,patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version',
        IOS_API: b'timestamp,UTC time,patient_id,MAC,phone_number,device_id,device_os,os_version,product,brand,hardware_id,manufacturer,model,beiwe_version',
    },
    IOS_LOG_FILE: {
        ANDROID_API: b'timestamp,UTC time,launchId,memory,battery,event,msg,d1,d2,d3,d4',  # ios-only datastream
        IOS_API: b'timestamp,UTC time,launchId,memory,battery,event,msg,d1,d2,d3,d4',
    },
    MAGNETOMETER: {
        ANDROID_API: b'timestamp,UTC time,x,y,z',  # ios-only datastream
        IOS_API: b'timestamp,UTC time,x,y,z',
    },
    POWER_STATE: {
        ANDROID_API: b'timestamp,UTC time,event',
        IOS_API: b'timestamp,UTC time,event,level',
    },
    PROXIMITY: {
        ANDROID_API: b'timestamp,UTC time,event',  # ios-only datastream
        IOS_API: b'timestamp,UTC time,event',
    },
    REACHABILITY: {
        ANDROID_API: b'timestamp,UTC time,event',  # ios-only datastream
        IOS_API: b'timestamp,UTC time,event',
    },
    # SURVEY_ANSWERS: {  # we don't chunk survey answers...
    #     ANDROID_API: b'question id,question type,question text,question answer options,answer',
    #     IOS_API:     b'question id,question type,question text,question answer options,answer',
    # },
    SURVEY_TIMINGS: {
        ANDROID_API: b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer',
        IOS_API: b'timestamp,UTC time,question id,survey id,question type,question text,question answer options,answer,event',
    },
    TEXTS_LOG: {
        ANDROID_API: b'timestamp,UTC time,hashed phone number,sent vs received,message length,time sent',
        IOS_API: b'timestamp,UTC time,hashed phone number,sent vs received,message length,time sent',
        # android-only datastream
    },
    WIFI: {
        ANDROID_API: b'timestamp,UTC time,hashed MAC,frequency,RSSI',
        IOS_API: b'timestamp,UTC time,hashed MAC,frequency,RSSI',
    }
}

REFERENCE_UPLOAD_HEADERS = {
    ACCELEROMETER: {
        ANDROID_API: b'timestamp,accuracy,x,y,z',
        IOS_API: b'timestamp,accuracy,x,y,z',
    },
    ANDROID_LOG_FILE: {
        ANDROID_API: b'THIS LINE IS A LOG FILE HEADER',
        IOS_API: b'THIS LINE IS A LOG FILE HEADER',  # android-only datastream
    },
    BLUETOOTH: {
        ANDROID_API: b'timestamp, hashed MAC, RSSI',
        IOS_API: b'timestamp, hashed MAC, RSSI',  # android-only datastream
    },
    OMNIRING: {
        ANDROID_API: b'PPG_red,PPG_IR,PPG_Green,IMU_Accel_x,IMU_Accel_y,IMU_Accel_z,IMU_Gyro_x,IMU_Gyro_y,IMU_Gyro_z,IMU_Mag_x,IMU_Mag_y,IMU_Mag_z,temperature,timestamp',
        IOS_API: b'PPG_red,PPG_IR,PPG_Green,IMU_Accel_x,IMU_Accel_y,IMU_Accel_z,IMU_Gyro_x,IMU_Gyro_y,IMU_Gyro_z,IMU_Mag_x,IMU_Mag_y,IMU_Mag_z,temperature,timestamp',
    },
    CALL_LOG: {
        ANDROID_API: b'hashed phone number,call type,timestamp,duration in seconds',
        IOS_API: b'hashed phone number,call type,timestamp,duration in seconds',  # android-only datastream
    },
    DEVICEMOTION: {
        ANDROID_API: b'timestamp,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
        # ios-only datastream
        IOS_API: b'timestamp,roll,pitch,yaw,rotation_rate_x,rotation_rate_y,rotation_rate_z,gravity_x,gravity_y,gravity_z,user_accel_x,user_accel_y,user_accel_z,magnetic_field_calibration_accuracy,magnetic_field_x,magnetic_field_y,magnetic_field_z',
    },
    GPS: {
        ANDROID_API: b'timestamp, latitude, longitude, altitude, accuracy',
        IOS_API: b'timestamp,latitude,longitude,altitude,accuracy',
    },
    GYRO: {
        ANDROID_API: b'timestamp,accuracy,x,y,z',
        IOS_API: b'timestamp,x,y,z',
    },
    IDENTIFIERS: {
        ANDROID_API: DEVICE_IDENTIFIERS_HEADER,
        IOS_API: DEVICE_IDENTIFIERS_HEADER,
    },
    IOS_LOG_FILE: {
        ANDROID_API: b'timestamp,launchId,memory,battery,event,msg,d1,d2,d3,d4',  # ios-only datastream
        IOS_API: b'timestamp,launchId,memory,battery,event,msg,d1,d2,d3,d4',
    },
    MAGNETOMETER: {
        ANDROID_API: b'timestamp,x,y,z',  # ios-only datastream
        IOS_API: b'timestamp,x,y,z',
    },
    POWER_STATE: {
        ANDROID_API: b'timestamp, event',
        IOS_API: b'timestamp,event,level',
    },
    PROXIMITY: {
        ANDROID_API: b'timestamp,event',  # ios-only datastream
        IOS_API: b'timestamp,event',
    },
    REACHABILITY: {
        ANDROID_API: b'timestamp,event',  # ios-only datastream
        IOS_API: b'timestamp,event',
    },
    # SURVEY_ANSWERS: {  # we don't chunk survey answers...
    #     ANDROID_API: b'question id,question type,question text,question answer options,answer',
    #     IOS_API:     b'question id,question type,question text,question answer options,answer',
    # },
    SURVEY_TIMINGS: {
        ANDROID_API: b'timestamp,question id,question type,question text,question answer options,answer',
        IOS_API: b'timestamp,question id,question type,question text,question answer options,answer,event',
    },
    TEXTS_LOG: {
        ANDROID_API: b'timestamp,hashed phone number,sent vs received,message length,time sent',
        IOS_API: None,  # ios-only datastream
    },
    WIFI: {
        ANDROID_API: b'hashed MAC, frequency, RSSI',
        IOS_API: b'hashed MAC, frequency, RSSI',  # android-only datastream
    }
}

CHUNK_EXISTS_CASE = "chunk_exists_case"
