# coding=utf-8
from datetime import datetime
from wit import Wit
from string import Template
from time import sleep
from collections import namedtuple
from pathlib import Path
import pandas as pd
import deepcut
import os
import glob
import pickle
import config

toq_key = config.toq_key
say_key = config.say_key
sub_key = config.sub_key
sec_key = config.sec_key
who_key = config.who_key

now_here = os.getcwd()


def get_file_name(dir_file):
    fn = os.path.basename(dir_file)
    fn_alone = os.path.splitext(fn)[0]
    return fn_alone


# df คือตาราง extend คือคำที่ให้เข้าใจว่าตารางนี้เปลี่ยนไปอย่างไร และเป็นชื่อโฟลเดอร์สำหรับเก็บไฟล์นี้ด้วย
def export_file(old_table, new_table, extend):
    file_name = os.path.basename(old_table)
    fn_no_extension = os.path.splitext(file_name)[0]
    path_here = os.getcwd()
    # ส่งออกตาราง df
    directory = os.path.join(path_here, extend)

    if not os.path.exists(directory):
        os.makedirs(directory)

    export_file_dir = os.path.join(directory, fn_no_extension + '_{!s}.csv'.format(extend))
    new_table.to_csv(export_file_dir, sep='\t', encoding='utf-8')
    print('ส่งออกไฟล์ {!s} แล้ว'.format(fn_no_extension + '_{!s}.csv'.format(extend)))


# เริ่มจากนำเข้า csv ที่ได้จาก txt ที่ export มาจาก line
# แล้วนำไปเปลี่ยนแปลงให้ได้ตารางที่ประกอบด้วย เวลาในการส่งข้อความ (time) ชื่อผู้ส่งข้อความ (name) และ ข้อความ (text)
def clean_table(file_path):
    # chat คือ ตารางที่มาจากตาราง csv ที่เราจะ clean
    chat = pd.read_csv(file_path)
    # chat_mod คือ ตารางที่มาจาก chat แต่ใส่ชื่อให้คอลัมน์ใหม่
    chat_mod = pd.DataFrame({'time': chat.ix[:, 0], 'name': chat.ix[:, 1], 'text': chat.ix[:, 2]})

    # ถ้าข้อมูลที่ส่งเข้ามาตัดอักษรห้าตัวข้างหน้าแล้วเป็นวันที่ จะถูกส่งกลับแค่วันที่
    # ส่วนข้อมูลอื่น ๆ ที่ไม่ใช่เงื่อนไขนี้ จะไม่ถูกทำอะไร ส่งกลับแบบเดิม
    def validate(date_text):
        try:
            datetime.strptime(date_text[5:], '%d/%m/%Y')
            b = date_text[5:]
            return b
        except ValueError:
            return date_text

    # ตรวจสอบข้อมูลที่ส่งเข้ามาว่าอยู่ในรูปแบบ '%H:%M' หรือไม่
    def tm(t):
        try:
            datetime.strptime(t, '%H:%M')
            return True
        except ValueError:
            return False

    # ตรวจสอบข้อมูลที่ส่งเข้ามาว่าอยู่ในรูปแบบ '%d/%m/%Y' หรือไม่
    def date(d):
        try:
            datetime.strptime(d, '%d/%m/%Y')
            return True
        except ValueError:
            return False

    # เอาข้อมูลในคอลัมน์ time ตัวที่มีชื่อวัน ตัดชื่อวันออก ตัวอื่น ๆ ไม่ทำไร แล้วใส่เป็น list
    na = []
    for vela in chat_mod['time']:
        k = validate(str(vela))
        na.append(k)

    # เอาข้อมูลในลิสต์ na มาดู
    for s in na:
        # ถ้าข้อมูลในลิสต์อยู่ในรูปแบบ '%H:%M'
        if tm(s):
            # ถ้าข้อมูลใน na ตำแหน่งที่อยู่ก่อนหน้า s อยู่ในรูปแบบ '%d/%m/%Y'
            if date(na[na.index(s) - 1]):
                # ให้เปลี่ยนข้อมูลตำแหน่ง s เป็น ข้อมูลตำแหน่งก่อนหน้า ตามด้วย วรรค ตามด้วย s ตามเดิม
                na[na.index(s)] = na[na.index(s) - 1] + " " + s
            # ถ้าข้อมูลใน na ตำแหน่งที่อยู่ก่อนหน้า s ถูกตัดท้าย 6 ตัวอักษร แล้วอยู่ในรูปแบบ '%d/%m/%Y'
            elif date(na[na.index(s) - 1][:-6]):
                # ให้เปลี่ยนข้อมูลตำแหน่ง s เป็น ข้อมูลตำแหน่งก่อนหน้า ที่ถูกตัดท้าย 6 ตัวอักษรแล้ว ตามด้วย วรรค
                # ตามด้วย s ตามเดิม
                na[na.index(s)] = na[na.index(s) - 1][:-6] + " " + s
            # ถ้าข้อมูลอยู่ในรูปแบบอื่น ๆ ไม่ต้องทำไร
            else:
                pass
    # เสร็จแล้วจะได้ na ที่มีสมาชิกอยู่ในรูปแบบ %d/%m/%Y %H:%M

    # time_mod คือคอลัมน์ที่มีวันที่อยู่หน้าเวลา ในรูปแบบ %d/%m/%Y %H:%M
    chat_mod['time_mod'] = pd.Series(na)

    # fd เป็นตารางที่มี 3 คอลัมน์
    fd = chat_mod[['time_mod', 'name', 'text']]

    # dfd เป็นตารางที่ลบ row ที่คอลัมน์ text ไม่มีค่า
    dfd = fd.dropna(subset=['text'])

    # ลิสต์เหล่านี้มาจากแต่ละคอลัมน์ของ dfd
    a1 = dfd['time_mod'].tolist()
    a2 = dfd['name'].tolist()
    a3 = dfd['text'].tolist()

    # นำ a1 a2 a3 มาสร้างตารางใหม่ ชื่อ df
    df = pd.DataFrame({'time': a1, 'name': a2, 'text': a3})

    export_file(file_path, df, 'cleaned')
    return df


def time_inter(ct):
    b1 = pd.Series(ct['time'])
    b2 = pd.Series(ct['time'])
    temp_vela = '%d/%m/%Y %H:%M'
    la = 0
    minute_set = []

    for _ in b1:
        try:
            c1 = datetime.strptime(b1[la - 1], temp_vela)
            c2 = datetime.strptime(b2[la], temp_vela)
            d1 = c2 - c1
            minute_set.append(d1)
            la = la + 1
        except KeyError:
            c1 = datetime.strptime(b1[la], temp_vela)
            d1 = c1 - c1
            minute_set.append(d1)
            la = la + 1

    # คอลัมน์ time_ans แสดงเวลาก่อนจะตอบ เป็นหน่วย วันตามด้วยเวลาแบบ 00:00:00
    time_ans = pd.Series(minute_set)

    # คอลัมน์ time_min แสดงเวลาก่อนจะตอบ เป็นหน่วย minute
    time = pd.DatetimeIndex(time_ans)
    time_min = (time.day - 1) * 24 * 60 + time.hour * 60 + time.minute
    return time_min


def sender_num(ct):
    # แปลงชื่อผู้ส่งข้อความเป็นตัวเลข
    ra = []
    name_set = set(ct['name'].tolist())
    name_list = list(name_set)
    for each_name in ct['name']:
        ra.append(name_list.index(each_name))
    return ra


def numb_text(ct):
    sii = 1
    yaa = []
    x = ct['name'].tolist()
    lal = 0
    # x คือ ลิสต์ของตัวเลขผู้ส่งข้อความ
    # พิจารณาตัวเลขผู้ส่งข้อความของแต่ละข้อความ
    for each_name in x:
        # n คือ เลขผู้ส่งข้อความที่สนใจ
        # na2 คือ สมาชิกตัวที่อยู่ก่อนหน้าหน้า n
        na2 = x[lal - 1]
        # ถ้า เลขผู้ส่งข้อความที่สนใจ เป็นตัวเดียวกับเลขของผู้ส่งข้อความที่อยู่ก่อนหน้า
        if each_name == na2:
            # เพิ่มค่า sii เดิม ลงใน yaa
            yaa.append(sii)
        # ถ้า เลขผู้ส่งข้อความที่สนใจ ไม่ใช่ตัวเดียวกับตัวเลขของผู้ส่งข้อความที่อยู่ก่อนหน้า
        elif each_name != na2:
            # ปรับ sii เป็น 1 แล้วเพิ่มเข้า sii
            sii = 1
            yaa.append(sii)
        # เปลี่ยนค่า sii สำหรับรอใส่ใน yaa ถ้าประโยคต่อไปเป็นผู้ส่งคนเดียวกัน
        sii = sii + 1
        # เปลี่ยนค่า lal เพื่อเป็นตัวนำไปคำนวณระบุตำแหน่งของตัวเลขผู้ส่งก่อนหน้า
        lal = lal + 1
    return yaa


def word_separation(text):
    # custom_dict = '/Users/bigmorning/Desktop/myword.txt'
    sep_text = deepcut.tokenize(text)
    join_sep_text = " ".join(sep_text)
    return join_sep_text


def extract_value(inp_text, wit_token):
    understanding = Wit(wit_token)
    deep = understanding.message(inp_text)

    try:
        intent_value = deep['data'][0]['__wit__legacy_response']['entities']['intent'][0]['value']
    except KeyError:
        try:
            intent_value = deep['entities']['intent'][0]['value']
        except KeyError:
            intent_value = deep['entities']
    return intent_value


def show_progress(mal, l):
    try:
        s0 = Template('เพิ่มค่า $value ในเซต $set')
        s1 = s0.substitute(value=mal, set=l)
    except TypeError:
        s0 = Template('เพิ่มค่า $value ในเซต $set')
        s1 = s0.substitute(value=str(mal), set=l)
    return print(s1)


def load_keep(extend, file_path, sv_wcs, sv_secs, sv_scs, sv_ws, sv_ts, sv_ss):
    directory = os.path.join(now_here, extend, get_file_name(file_path) + '_keep.txt')
    if not os.path.exists(directory):
        with open(directory, "wb") as fp:
            word_count_set = sv_wcs
            sen_count_set = sv_secs
            sub_count_set = sv_scs
            who_set = sv_ws
            toq_set = sv_ts
            say_set = sv_ss
            pickle.dump((word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set), fp)
        return word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set
    else:
        with open(directory, "rb") as fp:
            word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set = pickle.load(fp)
        return word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set


def save_keep(extend, file_path, word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set, n):
    directory = os.path.join(now_here, extend, get_file_name(file_path) + '_keep.txt')
    if n % 5 == 0:
        with open(directory, "wb") as fp:
            pickle.dump((word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set), fp)
    else:
        pass


def initial_assignment(file_path, ct):
    ia = namedtuple('type', 'wordCount senCount sAppear menWho senType doType')
    text = ct['text'].tolist()
    word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set = load_keep('analyse',
                                                                                        file_path,
                                                                                        [], [], [], [], [], [])
    for n, r in enumerate(text):
        if n == len(word_count_set):
            print('เริ่มวิเคราะห์ประโยคที่ {!s} : {!s}'.format(str(n), r))
            sep_word = word_separation(r)

            # นับคำใน text box
            word_count = len(sep_word.split())
            word_count_set.append(word_count)
            show_progress(word_count, 'word_count_set')

            # การสื่อสารใน text box เป็นกี่ประโยค มี 0, 1, มากกว่า 1
            sen_count = extract_value(sep_word, sec_key)
            sen_count_set.append(sen_count)
            show_progress(sen_count, 'sen_count_set')

            # ระบุประธานของประโยคหรือไม่
            sub_count = extract_value(sep_word, sub_key)
            sub_count_set.append(sub_count)
            show_progress(sub_count, 'sub_count_set')

            # ประโยคนี้พูดเกี่ยวกับตัวเอง หรือคู่สนทนา หรือทั้งสอง หรืออย่างอื่น
            who = extract_value(sep_word, who_key)
            who_set.append(who)
            show_progress(who, 'who_set')

            # ประโยคนั้นเป็นบอกเล่าหรือคำถาม
            toq = extract_value(sep_word, toq_key)
            toq_set.append(toq)
            show_progress(toq, 'toq_set')

            # การกระทำของประโยคนั้น
            say = extract_value(sep_word, say_key)
            say_set.append(say)
            show_progress(say, 'say_set')

            print("----------เสร็จสิ้นแถวที่ " + str(n) + " ----------")

            save_keep('analyse', file_path, word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set, n)

    df = pd.DataFrame({'name': ct['name'],
                       'text': ct['text'],
                       'wordCount': word_count_set,
                       'senCount': sen_count_set,
                       'sAppear': sub_count_set,
                       'menWho': who_set,
                       'senType': toq_set,
                       'doType': say_set})

    export_file(file_path, df, 'analyse')

    return ia(wordCount=word_count_set,
              senCount=sen_count_set,
              sAppear=sub_count_set,
              menWho=who_set,
              senType=toq_set,
              doType=say_set)


def som(file_path, ct):
    ia = namedtuple('type', 'wordCount senCount sAppear menWho senType doType')

    # หาว่าจะเรียกไฟล์ csv ตัวไหนมาซ่อม
    # เรียกจากไฟล์ที่ติด extend analyse
    ext = 'analyse'
    directory = os.path.join(now_here, ext)

    # path ที่อยู่ของไฟล์ csv ที่จะเรียกมาซ่อม
    call_csv = os.path.join(directory, get_file_name(file_path) + '_{!s}.csv'.format(ext))

    # เปิดไฟล์
    last_csv = pd.read_csv(call_csv, sep='\t')

    # แปลงแต่ละคอลัมน์เป็นลิสต์
    # แล้วเรียก load_keep
    word_count_set, sen_count_set, sub_count_set, who_set, toq_set, say_set = load_keep('anCom',
                                                                                        file_path,
                                                                                        last_csv['wordCount'].tolist(),
                                                                                        last_csv['senCount'].tolist(),
                                                                                        last_csv['sAppear'].tolist(),
                                                                                        last_csv['menWho'].tolist(),
                                                                                        last_csv['senType'].tolist(),
                                                                                        last_csv['doType'].tolist())

    an_key = {'senCount': sec_key, 'sAppear': sub_key, 'menWho': who_key, 'senType': toq_key, 'doType': say_key}
    type_set = {'senCount': sen_count_set, 'sAppear': sub_count_set, 'menWho': who_set, 'senType': toq_set,
                'doType': say_set}

    text = ct['text'].tolist()
    for v in list(type_set.values()):
        for n, i in enumerate(v):
            while i == str({}) or i == {}:
                key = list(type_set.keys())[list(type_set.values()).index(v)]
                token_word_list = deepcut.tokenize(text[n])
                token_word = ' '.join(token_word_list)
                v[n] = extract_value(token_word, an_key[key])
                print('เปลี่ยนเซตว่างใน {!s} แถวที่ {!s} เป็น {!s}'.format(key, str(n), v[n]))
                if v[n] == {}:
                    print('ทำอีกรอบ พิจารณา ประโยค : ' + token_word)
                    sleep(20)
                else:
                    save_keep('anCom',
                              file_path,
                              word_count_set,
                              sen_count_set,
                              sub_count_set,
                              who_set,
                              toq_set,
                              say_set,
                              5)
                    i = v[n]

    df = pd.DataFrame({'name': ct['name'],
                       'text': ct['text'],
                       'wordCount': word_count_set,
                       'senCount': sen_count_set,
                       'sAppear': sub_count_set,
                       'menWho': who_set,
                       'senType': toq_set,
                       'doType': say_set})

    export_file(file_path, df, 'anCom')

    return ia(wordCount=word_count_set,
              senCount=sen_count_set,
              sAppear=sub_count_set,
              menWho=who_set,
              senType=toq_set,
              doType=say_set)


all_file = glob.glob(now_here + '/csv_line/*.csv')
all_file_analyse = os.listdir(now_here + '/analyse')
all_file_anCom = os.listdir(now_here + '/anCom')

# จะทำ analyse ให้เสร็จก่อน
for file in all_file:
    new_table = clean_table(file)
    min_time = time_inter(new_table)
    num_sender = sender_num(new_table)
    num_text = numb_text(new_table)
    if get_file_name(file) + '_analyse.csv' not in all_file_analyse:
        print('ไม่มี {!s} ในโฟลเดอร์ analyse ดังนั้นจะเริ่ม analyse'.format(get_file_name(file)))
        path = Path(file)
        print('กำลังดู ' + str(path))
        initial_assignment(file, new_table)
    elif get_file_name(file) + '_anCom.csv' not in all_file_anCom:
        print('มี {!s} ในโฟลเดอร์ analyse แล้ว ดังนั้นจะไม่ analyse อีก'.format(get_file_name(file)))
        print('ไม่มี {!s} ในโฟลเดอร์ anCom ดังนั้นจะเริ่ม analyse'.format(get_file_name(file)))
        analyse = som(file, new_table)

        new_table['min'] = pd.Series(min_time)
        new_table['num_sender'] = pd.Series(num_sender)
        new_table['num_text'] = pd.Series(num_text)
        new_table['word_count'] = pd.Series(analyse.wordCount)
        new_table['sen_count'] = pd.Series(analyse.senCount)
        new_table['s_appear'] = pd.Series(analyse.sAppear)
        new_table['men_who'] = pd.Series(analyse.menWho)
        new_table['sen_type'] = pd.Series(analyse.senType)
        new_table['do_type'] = pd.Series(analyse.doType)

        export_file(new_table, 'complete')

        with pd.option_context('display.max_rows', None, 'display.max_columns', 6):
            print(new_table)
    else:
        pass
