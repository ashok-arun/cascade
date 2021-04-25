import socket, cv2, time, pickle, struct, sys
import numpy as np
import cascade_py

capi = cascade_py.ServiceClientAPI()

# def receive_frames_from_server(host_ip, port):
#     receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     receive_socket.bind((host_ip, port))
#     print("[SOCKET] bind complete")

#     receive_socket.listen(10)
#     print("[SOCKET] listening ")
#     conn, addr = receive_socket.accept()

#     data = b""
#     payload_size = struct.calcsize("Q")
#     print("payload_size: {}".format(payload_size))
#     # capi.create_object_pool('/Ithaca/farm1','VCSS',str(0))
#     while True:
#         try:
#             while len(data) < payload_size:
#                 # use 4b buffer
#                 data += conn.recv(4096)
#                 print(len(data))

#             print("Done Recv: {}".format(len(data)))
#             packed_msg_size = data[:payload_size]
#             data = data[payload_size:]
#             msg_size = struct.unpack("Q", packed_msg_size)[0]
#             while len(data) < msg_size:
#                 data += conn.recv(4096)
#             frame_data = data[:msg_size]
#             data = data[msg_size:]
#             frame_packet = pickle.loads(frame_data)
#             source, idx = frame_packet['header']
#             image_frame = frame_packet['frame']
#             print(source)
#             print(idx)

#             # prepare image: BGR -> RGB
#             image_frame = cv2.imdecode(image_frame, cv2.IMREAD_COLOR)
#             image_frame = image_frame.astype(np.single)  # 32-bit float
#             img_rgb = image_frame[:,:,::-1]
#             img_rgb = cv2.resize(img_rgb, (224,224))

#             # put to subgroup VCSS
#             cascade_frame = image_frame.tobytes()
#             ret = capi.put('VCSS', '/cow_dfg1/Ithaca/farm1/'+str(idx), cascade_frame, 0, 0, True)
#             # ret = capi.put('VCSS', '/image_pipeline/'+str(idx)+'/lo', cascade_frame, 0, 0)
#             print(ret.get_result())
#         except Exception as e:
#             print('exception occured: {}'.format(e))
#     receive_socket.close()


def process_frame():
    name = "/home/yy354/workspace/cascade/build/src/applications/demos/dairy_farm/cow_frame"
    for idx in range(1,3):
        img_name = name+str(idx)+".jpg"
        image_frame = cv2.imread(img_name)
        # img = cv2.imdecode(image_frame, cv2.IMREAD_COLOR)
        img = cv2.resize(image_frame, (352, 240))
        _, image_frame = cv2.imencode('.jpg', img)
        image_frame = image_frame / 255
        image_frame = image_frame.astype(np.single)
        img_rgb = image_frame[:,:,::-1]

        # put to subgroup VCSS
        cascade_frame = img_rgb.tobytes()
        ret = capi.put('VCSS', '/dairy_farm/front_end/img'+str(idx), cascade_frame, 0, 0)
        # ret = capi.put('VCSS', '/cow_dfg1/flower/'+str(idx), cascade_frame, 0, 0, True)

        # print(ret.get_result())
        time.sleep(1)

if __name__ == '__main__':
    host_ip = ''
    port = 8080
    # receive_frames_from_server(host_ip, port)
    process_frame()

    time.sleep(10)
