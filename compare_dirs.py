import os
import re
import pandas as pd
import shutil
import os.path as path
import time
import datetime
import hashlib

from pandas.testing import assert_frame_equal
import openpyxl

import timeit
from zipfile import ZipFile
from rarfile import RarFile
import patoolib
import py7zr

from io import DEFAULT_BUFFER_SIZE

import PySimpleGUI as sg
import time

from pathlib import Path
from multiprocessing.pool import ThreadPool
import sqlite3


#LONG WAYS DATAFRAME AND METADATA BY PATHS
def metadata_file(path_file):
    """
    path_file ('str'): full path files
    
    This function return metadata in tuple - name file, size, path file, date and extinsion. All data is 'str'.
    
    """
    basepath, filename = os.path.split(path_file)
    name_file, extension = os.path.splitext(filename)# name and extension file
    extension = extension[1:]
    metadata = os.stat(os.path.abspath(path_file))
    file_size = metadata.st_size #size file

    #create date file
    full_time = time.localtime(metadata.st_mtime) 
    data_time = datetime.datetime(full_time.tm_year, full_time.tm_mon, full_time.tm_mday, full_time.tm_hour, full_time.tm_min, full_time.tm_sec) 
    date_create_file = data_time.strftime("%d-%m-%Y %H:%M:%S") # date - day-month-year hours:minutes:seconds
    
    new_tup = (name_file, file_size, basepath, date_create_file, extension)

    return new_tup

def df_with_metadata(pathname):
    data_metafiles_data = []
    list_long_ways = []
    """
    pathname ('str') - file path

    Function run by path and return list with tuples metadata files and list with long path. If archive in path - function open archive. but don`t extract.
    """
    for path, dirs, files in os.walk(pathname):
        print(path)
        if len(path) > 240: #find long way
            list_long_ways += [path]
        for f in files:
            #print(f) #debug
            file_path = path + '\\' + f
            
            if len(file_path) > 240:
                list_long_ways += [file_path]
                continue

            if re.search('.rar', f):
                try:
                    with RarFile(f) as rf:
                        for file_path in rf.namelist():
                            if file_path[-1] == '\\' and file_path[-1] == '/' and len(path + file_path) > 250: #if file_path is path
                                list_long_ways += [path + file_path]
                                continue
                            if file_path[-1] != '\\' and file_path[-1] != '/': #if file_path is file, not way
                                data_metafiles_data += [metadata_file(os.path.abspath(os.path.join(path, file_path)))]
                except:
                    if file_path not in list_long_ways and len(file_path) > 240:
                        list_long_ways += [file_path]

            if re.search('.zip', f):
                try:
                    with RarFile(f) as rf:
                        for file_path in rf.namelist():
                            if file_path[-1] == '\\' and file_path[-1] == '/' and len(path + file_path) > 250: #if file_path is path
                                    list_long_ways += [path + file_path]
                            if file_path[-1] != '\\' and file_path[-1] != '/': #if file_path is file, not way
                                data_metafiles_data += [metadata_file(os.path.abspath(os.path.join(path, file_path)))]
                except:
                    if file_path not in list_long_ways and len(file_path) > 240:
                        list_long_ways += [file_path]

            if re.search('.7z', f):
                try:
                    with py7zr.SevenZipFile(f, 'r') as zip7:
                        for fname, bio in zip7.readall().items():
                            if len(fname) > 250:
                                list_long_ways += [fname]
                            data_metafiles_data += [metadata_file(os.path.abspath(os.path.join(path, fname)))]
                except:
                    if file_path not in list_long_ways and len(file_path) > 240:
                        list_long_ways += [file_path]
            
            else:
                data_metafiles_data += [metadata_file(file_path)]

    return data_metafiles_data, list_long_ways


#HASH FILE
def get_hash(file):
    """
    file ('str') - full path with file mane and extention

    Function return hash file and full path with file mane and extention
    """
    if file[4] == 'SHA256':
        sha = hashlib.sha256()
    if file[4] == 'SHA1':
        sha = hashlib.sha1()
    if file[4] == 'MD5':
        sha = hashlib.md5()
    
    with open(file[0], mode='rb') as fl:
        chunk = fl.read(DEFAULT_BUFFER_SIZE)
        while chunk:
            sha.update(chunk)
            chunk = fl.read(DEFAULT_BUFFER_SIZE)
    return sha.hexdigest(), file[0], file[1], file[2], file[3]


def get_same_and_diff_files(df1, df2, df_for_hash, type_hash):
    if __name__ == '__main__':
        x_files = [(os.path.abspath(way + '\\' + name + '.' + extention), size, date, type_df, type_hash) for way, name, extention, size, date, type_df, type_hash in zip(df_for_hash.dirname_x, df_for_hash.filename, df_for_hash.extension_x, df_for_hash.file_size, df_for_hash.date_create_file, 'x'*len(df_for_hash.dirname_x.values), [type_hash]*len(df_for_hash.dirname_x.values))]
        y_files = [(os.path.abspath(way + '\\' + name + '.' + extention), size, date, type_df, type_hash) for way, name, extention, size, date, type_df, type_hash in zip(df_for_hash.dirname_y, df_for_hash.filename, df_for_hash.extension_y, df_for_hash.file_size, df_for_hash.date_create_file, 'y'*len(df_for_hash.dirname_y.values), [type_hash]*len(df_for_hash.dirname_y.values))]
        files = [(file, size, date, type_df, type_hash) for file, size, date, type_df, type_hash in [*x_files, *y_files]]

        number_of_workers = os.cpu_count()

        print('Programm count hash files')
        with ThreadPool(number_of_workers) as pool:
            files_hash = pool.map(get_hash, files)

        #Search same and difference files with hash
        print('Search same and difference files, part 2 of 2')
        print('Please wait')
        list_x = []
        list_y = []
        for hash, way, size, date, type_df in files_hash:
            file_path, full_filename = os.path.split(way)
            filename, filextention = os.path.splitext(full_filename)
            extension = filextention[1:]
            row = (filename, size, date, file_path, extension, hash)

            if type_df == 'x':
                list_x += [row]

            if type_df == 'y':
                list_y += [row]
        df_x = pd.DataFrame([*list_x], columns=['filename', 'file_size', 'date_create_file', 'dirname', 'extension', 'hash_file'])
        df_y = pd.DataFrame([*list_y], columns=['filename', 'file_size', 'date_create_file', 'dirname',  'extension', 'hash_file'])

        df_same = df_x.merge(df_y, how = 'inner' , indicator=False, on=['filename', 'file_size','date_create_file','hash_file'])

        #different files. Check rows tith same name file, size, date, dirname and extension. Create srt mask for df1 and df2
        df_str_same = pd.DataFrame([((filename+str(file_size)+date_create_file+dirname_x+extension_x),(filename+str(file_size)+date_create_file+dirname_y+extension_y)) 
        for filename, file_size, date_create_file,dirname_x, extension_x, dirname_y, extension_y 
        in zip(df_same.filename, df_same.file_size, df_same.date_create_file, df_same.dirname_x, df_same.extension_x, df_same.dirname_y, df_same.extension_y)], columns=['x', 'y'])
        
        df_str_df1 = pd.DataFrame([(filename+str(file_size)+date_create_file+os.path.abspath(dirname)+extension) for filename, file_size, date_create_file, dirname, extension 
                in zip(df1.filename, df1.file_size, df1.date_create_file, df1.dirname, df1.extension)], columns=['df1'])
        df_str_df2 = pd.DataFrame([(filename+str(file_size)+date_create_file+os.path.abspath(dirname)+extension) for filename, file_size, date_create_file, dirname, extension 
                in zip(df2.filename, df2.file_size, df2.date_create_file, df2.dirname, df2.extension)], columns=['df2'])
        
        df_diff1 = df1[~df_str_df1.df1.isin(df_str_same.x)].reset_index(drop=True)
        df_diff2 = df2[~df_str_df2.df2.isin(df_str_same.y)].reset_index(drop=True)

        return df_same, df_diff1, df_diff2


sg.theme('BlueMono')
w, h = sg.Window.get_screen_size()

#TAB 'Сomputing'
tab0_layout = [[sg.Text('Hash method:'), sg.Radio('MD5','sha'), sg.Radio('SHA1','sha'), sg.Radio('SHA256','sha',default=True),
                    sg.Text('Note:', font = ("Arial", 10, 'bold')), sg.Text('choose one (!) hash method, default SHA256.')], 
               [sg.Text('Extension of the results files:'), sg.Checkbox('csv', default=True), sg.Checkbox('xlsx'), 
                    sg.Text('    Note:', font = ("Arial", 10, 'bold')), sg.Text('choose one or all extentions. Default csv, you cannot cancel this extension.')],

    [sg.Text('The initial path to save the results:'), sg.InputText(size = (65, 5), enable_events=True), sg.FolderBrowse()],
    [sg.Text('Path 1:'), sg.InputText(key='foldername1', size = (65, 5), enable_events=True), sg.FolderBrowse(),
         sg.Text('Path 2:'), sg.InputText(key='foldername2', size = (65, 5), enable_events=True), sg.FolderBrowse()],
    [sg.Text('OR  (you can choose summary files)')],
    [sg.Text('File 1:  '), sg.InputText(size = (65, 5), enable_events=True), sg.FileBrowse(), sg.Text('File 2: '), sg.InputText(size = (65, 5), enable_events=True), sg.FileBrowse()],
    
    [sg.Frame('Output', layout = [[sg.Output(key='-output-', size=(w, h//(h//15))) ]]) ],
    [sg.Frame('Progress', layout = [[sg.ProgressBar(7, orientation='h', size=(30, 10), key='progressbar')]]) ],
    [sg.Submit('Start'),sg.Cancel()]]

#TAB 'Long paths'
headings_ways = ['directory name']
ways_cols_width = [w//20]
tab1_layout = [[sg.Text('This tab contain long paths by two path. Programm don`t analyse files with long path.')],
                [sg.Text('Path 1:'), sg.Text(key='foldername1_long_paths'), sg.Text('Path 2:'), sg.Text(key='foldername2_long_paths')],
                [sg.Table(values=[], headings=headings_ways, col_widths=ways_cols_width, auto_size_columns=False, enable_events=True, num_rows= w//38,
                    display_row_numbers=True, justification='right', key='table_long_way1', vertical_scroll_only=False),
                sg.Table(values=[], headings=headings_ways, col_widths=ways_cols_width, auto_size_columns=False, enable_events=True, num_rows= w//38,
                    display_row_numbers=True, justification='right', key='table_long_way2', vertical_scroll_only=False)]
]

#TAB 'Summary files'
headings_data = ['filename', 'file size', 'directory name', 'date create file', 'extension']

tab2_layout = [[sg.Text('This tab contain summary by two path with files.')],
                [sg.Text('Path 1:'),  sg.Text(key='foldername1_summary'), sg.Text('Path 2:'), sg.Text(key='foldername2_summary')],
                [sg.Table(values=[], headings=headings_data, auto_size_columns=True, num_rows= w//38,
                    display_row_numbers=True, justification='right', key='table_data_df1', vertical_scroll_only=False),
                sg.Table(values=[], headings=headings_data, auto_size_columns=True, num_rows= w//38,
                    display_row_numbers=True, justification='right', key='table_data_df2', vertical_scroll_only=False)]
]

#TAB 'Same files'
headings_same_data = ['filename', 'file size', 'date create file', 'directory name path1', 'extention path1', 'hash file', 'directory name path2', 'extention path2']

tab3_layout = [[sg.Text('Same files from two paths.')],
                [sg.Text('    Note:', font = ("Arial", 10, 'bold')), sg.Text('if you want to select several lines manually, then hold down the button "Ctrl" and click on the line with the left mouse button.')],
                [sg.Table(values=[], headings=headings_same_data, auto_size_columns=True, num_rows= w//48,
                    display_row_numbers=True, justification='right', key='table_data_same', vertical_scroll_only=False)], 
                [sg.Text(key='-same_table_comm-')],
                [sg.Text('Delete in:'), sg.Radio('Path 1','delete', default=True, key='-delete_by_path1-'), sg.Radio('Path 2','delete', key='-delete_by_path2-')],
                #[sg.Button('Select all for delete'), sg.Button('Deselect for delete')],
                [sg.Submit('DELETE')]
]

#TAB 'Difference files'
headings_diff_data = ['filename', 'file size', 'directory name', 'date create file', 'extension']

tab4_layout = [[sg.Text('Different files from two paths.')],
                [sg.Text('Path for copy:'), sg.InputText(size = (65, 5), enable_events=True), sg.FolderBrowse()],
                [sg.Text('    Note:', font = ("Arial", 10, 'bold')), sg.Text('if you want to select several lines manually, then hold down the button "Ctrl" and click on the line with the left mouse button.')],
                [sg.Table(values=[], headings=headings_diff_data, auto_size_columns=True, num_rows= w//48,
                    display_row_numbers=True, justification='right', key='table_data_diff1', vertical_scroll_only=False),
                sg.Table(values=[], headings=headings_diff_data, auto_size_columns=True, num_rows= w//48,
                    display_row_numbers=True, justification='right', key='table_data_diff2', vertical_scroll_only=False)],
                [sg.Text(key='-diff1_table_comm-'), sg.Text(key='-diff2_table_comm-')],
                [sg.Text('Copy from:'), sg.Radio('Path 1','copy', default=True, key='-copy_from_path1-'), sg.Radio('Path 2','copy', key='-copy_from_path2-')],
                #[sg.Button('Select all for copy'), sg.Button('Deselect for copy')],
                [sg.Submit('COPY'), sg.Text('Please choose path for copy.', font = ("Arial", 10, 'bold'), key='-diff_tables_comm-')]
]



layout = [[[sg.Text('Please close all other programs on your computer. This will help to calculate everything quickly.', font = ("Arial", 12, 'bold'))],
    
    [sg.Text('1. Choose path for save compare results. The entered directories (path1 and path2) will be saved in this directory and they can be opened next time.'),
     ],
     [sg.Text('2. To compare files, select directories. The files of the first directory are checked (they are new), the files of the second are considered a database (they are already in the system).\n You can select summary files if they already exist. Like with directories file 1 - incoming, file 2 - database.'),
     ],
     [sg.Text('Note:', font = ("Arial", 10, 'bold')),sg.Text('You must have access to all the selected directories (for delete and copy files).'),
     ],
     [sg.Text('Attention:', font = ("Arial", 10, 'bold')),sg.Text('If you selected a file AND directories, then the program will use the file.'),
     ],
     [sg.Text('3. Click "Start".'),
     ],
           sg.TabGroup([[sg.Tab('Сomputing', tab0_layout),
                         sg.Tab('Long paths', tab1_layout, visible = False, key='long_path'),
                         sg.Tab('Summary files', tab2_layout, visible = False, key='summary'),
                         sg.Tab('Same files', tab3_layout, visible = False, key='same_f'),
                         sg.Tab('Difference files', tab4_layout, visible = False, key='diff_f')]], key = '-TabGroup-', size=(w, h - (h//6)))]]

window = sg.Window('Compare files by two path', layout, resizable=True, size=(1250, 750)).Finalize()
window.Maximize()

progress_bar = window['progressbar']

while True:
    event, values = window.read(timeout=10)
    #print(event, values)

    #values -  0: MD5, 1: SHA1, 2: SHA256, 3: csv, 4: xlsx, 5 :home_folder, 'foldername1': Path1, 'foldername2': Path2, 6: file1, 7: file2
    if event in (None, 'Exit', 'Cancel'):
        break
    if values[5] == '' and event == 'Start': #if path for save result files don`t choose
        print('Please choose the initial path to save the results.')

    if values[5] != '' and event != 'Start':
        home_foldername = values[5] + '/compare_files'
        try:
            with open(home_foldername + '/cash_paths.txt', 'r') as f: #if programm opened before
                list_paths = f.read().split('\n')
                foldername1 = values['foldername1'] = list_paths[0]
                foldername2 = values['foldername2'] = list_paths[1]

                window['foldername1'].update(foldername1)
                window['foldername2'].update(foldername2)
        except:
            foldername1 = ''
            foldername2 = ''   
    
    if values['foldername1'] != '':
        if foldername1 == '' or foldername1 != values['foldername1']:
                foldername1 = values['foldername1']
                window['foldername1'].update(foldername1)

    if values['foldername2'] != '':
        if foldername2 == '' or foldername2 != values['foldername2']:
            foldername2 = values['foldername2']
            window['foldername2'].update(foldername2)

    if event == 'Start':
        if foldername1 == '' and values[6] == '':
            print('Please coose Path1 or File 1')

        if foldername2 == '' and values[7] == '':
            print('Please choose Path2 or File 2')

        if os.path.isdir(home_foldername) == False: #if programm opened first time
            os.mkdir(home_foldername)
            foldername1 = values['foldername1']
            foldername2 = values['foldername2']


        if ((foldername1 != '' or values[6] != '') and (foldername2 != '' or values[7] != '')) and (values[0] is True or values[1] is True or values[2] is True):
            with open(home_foldername + '\\cash_paths.txt', 'w') as f: #save paths in file for next work
                f.writelines("%s\n" % line for line in [foldername1, foldername2])
            
            
            #CHECHING LONG PATHS AND GET SUMMARY PATHS
            print('Run by:')
            progress_bar.UpdateBar(0)
            #for path 1
            if values[6] == '':
                list_df1, list_df_long_paths1 = df_with_metadata(foldername1)
                df1  = pd.DataFrame([*list_df1], columns=['filename', 'file_size', 'dirname', 'date_create_file', 'extension'])
                #df1.filename = df1['filename'].str.encode(b"utf-8")
                df1.to_csv(home_foldername+'\\all_files_path1.csv', encoding='utf8', sep=';')

                df_long_paths1  = pd.DataFrame([*list_df_long_paths1], columns=['dirname']).drop_duplicates().reset_index(drop=True)
                if len(df_long_paths1) > 0:
                    sg.PopupOK(' The program (a) finds long way(s) in Path 1. You can see results in tab "Long paths".')
                    window['table_long_way1'].update(values = [list(f) for f in df_long_paths1.values])
                    df_long_paths1.to_csv(home_foldername+'\\long_ways_path1.csv', encoding='utf8', sep=';')
            else:
                df1 = pd.read_csv(values[6],sep=";")
                df_long_paths1 = []

            progress_bar.UpdateBar(1)
            #for path 2
            if values[7] == '':
                list_df2, list_df_long_paths2 = df_with_metadata(foldername2)
                df2  = pd.DataFrame([*list_df2], columns=['filename', 'file_size', 'dirname', 'date_create_file', 'extension'])
                df2.to_csv(home_foldername+'\\all_files_path2.csv', encoding='utf8', sep=';')            
                
                df_long_paths2  = pd.DataFrame([*list_df_long_paths2], columns=['dirname']).drop_duplicates().reset_index(drop=True)
                if len(df_long_paths2) > 0:
                    sg.PopupOK(' The program (a) finds long way(s) in Path 2. You can see results in tab "Long paths".')
                    window['table_long_way2'].update(values = [list(f) for f in df_long_paths2.values])
                    df_long_paths2.to_csv(home_foldername +'\\long_ways_path2.csv', encoding='utf8', sep=';')
            else:
                df2 = pd.read_csv(values[7],sep=";")
                df_long_paths2 = []

            #activate or nor tab long path
            if len(df_long_paths1) > 1 or len(df_long_paths2) > 1: #visible tab or not
                window['foldername1_long_paths'].update(foldername1)
                window['foldername2_long_paths'].update(foldername2)
                window['long_path'].update(visible = True)
            else:
                window['long_path'].update(visible = False)

            progress_bar.UpdateBar(2)
            #activate tab summary
            window['foldername1_summary'].update(foldername1)
            window['foldername2_summary'].update(foldername2)
            window['table_data_df1'].update(values = [list(f) for f in df1.values])
            window['table_data_df2'].update(values = [list(f) for f in df2.values])
            window['summary'].update(visible = True)
             
            
            #SAME AND DIFFERENCE
            progress_bar.UpdateBar(3)
            #Search same and difference files for count hash
            #case 1: name1=name2, size1=size2, date1=date2 => may be same if hash1=hash2 or different if hash1 != hash2

            print('Search same and difference files, part 1 of 2')          
            df_first_case = df1.merge(df2, how = 'inner' ,indicator=False, on=['filename', 'file_size', 'date_create_file'])
            progress_bar.UpdateBar(4)

            #set hash type
            if values[2] == True:
                type_hash = 'SHA256'
            elif values[0] == True:
                type_hash = 'MD5'
            elif values[1] == True:
                type_hash = 'SHA1'
            print('...')


            progress_bar.UpdateBar(5)
            df_first_case_same, df_diff1, df_diff2 = get_same_and_diff_files(df1, df2, df_first_case, type_hash)
            df_first_case_same.to_csv(home_foldername+'\\same_files_by_two_paths.csv', encoding='utf8', sep=';')
            df_diff1.to_csv(home_foldername+'\\different_files_path_1.csv', encoding='utf8', sep=';')
            df_diff2.to_csv(home_foldername+'\\different_files_path_2.csv', encoding='utf8', sep=';') 

            progress_bar.UpdateBar(6)
            if values[4] == True: #resaults to xlsx
                try:
                    df_first_case_same.to_excel("./same_files.xlsx")
                    with pd.ExcelWriter("./long_paths_files.xlsx") as writer:
                        df_long_paths1.to_excel(writer, sheet_name='long_paths_1')
                        df_long_paths2.to_excel(writer, sheet_name='long_paths_2')
                    with pd.ExcelWriter("./different_files.xlsx") as writer:
                        df_diff1.to_excel(writer, sheet_name='different_files_1')
                        df_diff2.to_excel(writer, sheet_name='different_files_2')
                    with pd.ExcelWriter("./summary_files.xlsx") as writer:
                        df1.to_excel(writer, sheet_name='files_paths_1')
                        df2.to_excel(writer, sheet_name='files_paths_2')
                except:
                    print('Unfortunately, the program cannot upload the results to excel.')

            progress_bar.UpdateBar(7)
            window['table_data_same'].update(values = [list(f) for f in df_first_case_same.values])
            window['same_f'].update(visible = True)
            window['table_data_diff1'].update(values = [list(f) for f in df_diff1.values])
            window['table_data_diff2'].update(values = [list(f) for f in df_diff2.values]) 
            window['diff_f'].update(visible = True)

            print('See the results on other tabs.')

    #delete files
    if values['table_data_same'] != '' and event == 'DELETE': #delete files in tab same files
        #window['-same_table_comm-'].update(values['table_data_same'])
        if values['-delete_by_path1-'] == True: #delete in path 1
            data_select_for_delete1 = [(df_first_case_same.dirname_x[row]+'\\'+df_first_case_same.filename[row]+'.'+df_first_case_same.extension_x[row]) for row in values['table_data_same']]
            window['-same_table_comm-'].update(data_select_for_delete1)

        if values['-delete_by_path2-'] == True: # or delete in path 2
            data_select_for_delete2 = [(df_first_case_same.dirname_y[row]+'\\'+df_first_case_same.filename[row]+'.'+df_first_case_same.extension_y[row]) for row in values['table_data_same']]
            window['-same_table_comm-'].update(data_select_for_delete2)

    #copy files
    path_for_copy = values['Browse4']
    if values['Browse4'] != "":
        window['-diff_tables_comm-'].update('')
        if values['table_data_diff1'] != '' and event == 'COPY' and values['-copy_from_path1-'] == True:
            data_select_for_copy1 = [(df_diff1.dirname[row]+'\\'+df_diff1.filename[row]+'.'+df_diff1.extension[row]) for row in values['table_data_diff1']]
            window['-diff1_table_comm-'].update(data_select_for_copy1)
            window['-diff2_table_comm-'].update('')

        if values['table_data_diff2'] != '' and event == 'COPY' and values['-copy_from_path2-'] == True:
            data_select_for_copy2 = [(df_diff2.dirname[row]+'\\'+df_diff2.filename[row]+'.'+df_diff2.extension[row]) for row in values['table_data_diff2']]
            window['-diff2_table_comm-'].update(data_select_for_copy2)
            window['-diff1_table_comm-'].update('')

            #window['-diff2_table_comm-'].update(values['table_data_diff2'])

            
window.close()