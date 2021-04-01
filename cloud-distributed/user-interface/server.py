from flask import Flask, request, render_template, send_file, after_this_request
from werkzeug.utils import secure_filename
import requests, tarfile, os
server_IP=os.environ["MANAGER_NODE_IP"]
download_IP=os.environ["ACCESS_NODE_IP"]

app = Flask(__name__)

@app.route("/")
def index(flag='',msg=''):
    if(flag=='create_user'):
        return render_template('index.html', create_user=msg)
    if(flag=='delete_user'):
        return render_template('index.html', delete_user=msg)
    if(flag=='view_data'):
        return render_template('index.html', view_data=msg)
    if(flag=='file_upload'):
        return render_template('index.html', file_upload=msg)
    if(flag=='file_download'):
        return render_template('index.html', file_download=msg)
    if(flag=='file_delete'):
        return render_template('index.html', file_delete=msg)
    if(flag=='dir_upload'):
        return render_template('index.html', dir_upload=msg)
    if(flag=='dir_download'):
        return render_template('index.html', dir_download=msg)
    if(flag=='dir_delete'):
        return render_template('index.html', dir_delete=msg)
    return render_template('index.html')

def add_user(userid=None,gkey=None):
    if userid==None or userid=='':
        return "Please enter the username"
    if gkey==None or gkey=='':
        return "Please enter the greyfish key"
    data = {'user_id':userid,"gkey":gkey}
    r = requests.post("https://"+server_IP+":2443/grey/create_user", json = data, verify=False)
    print(r.text)
    return r.text

def remove_user(userid=None,gkey=None):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    data = {'user_id':userid,"gkey":gkey}
    r = requests.post("https://"+server_IP+":2443/grey/delete_user", json = data, verify=False)
    print(r.text)
    return r.text

def upload_file(userid=None,gkey=None,file=None,dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if file==None or file=='':
        print("Please enter the filepath")
        return "Please enter the filepath"

    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload/"+gkey+"/"+userid,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload/"+gkey+"/"+userid+"/"+dir,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    print(r.text)
    return r.text

def download_file(userid=None,gkey=None,file=None,ddir=None,dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if file==None or file=='':
        print("Please enter the filepath")
        return "Please enter the filepath"
    if ddir==None or ddir=='':
       print("Please enter the path for download directory with the filename to be saved")
       return "Please enter the path for download directory with the filename to be saved"

    if dir=='':
        r=requests.get("https://"+download_IP+":3443/grey/grey/"+gkey+"/"+userid+"/"+file, verify=False)
    else:
        r=requests.get("https://"+download_IP+":3443/grey/grey/"+gkey+"/"+userid+"/"+file+"/"+dir, verify=False)
    
    
    if "INVALID" in r.text and len(r.text)<40:
        print(r.text)
        return r.text
    else:
        with open(ddir,'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return 'File downloaded successfully'

def delete_file(userid=None,gkey=None,file=None,dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if file==None or file=='':
        print("Please enter the filename")
        return "Please enter the filename"

    if dir=='':
        r=requests.get("https://"+server_IP+":2443/grey/delete_file/"+gkey+"/"+userid+"/"+file, verify=False)
    else:
        r=requests.get("https://"+server_IP+":2443/grey/delete_file/"+gkey+"/"+userid+"/"+file+"/"+dir, verify=False)
    print(r.text)
    return r.text

def upload_dir(userid=None,gkey=None,file=None,dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if file==None or file=='':
        print("Please select the file")
        return "Please select the file"

    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload_dir/"+gkey+"/"+userid,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload_dir/"+gkey+"/"+userid+"/"+dir,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    print(r.text)
    return r.text

def upload_replace_dir(userid=None,gkey=None,file=None,dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if file==None or file=='':
        print("Please select the file")
        return "Please select the file"

    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload_replace_dir/"+gkey+"/"+userid,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload_replace_dir/"+gkey+"/"+userid+"/"+dir,data={"filename":secure_filename(file.filename)},files={'file':file}, verify=False)
    print(r.text)
    return r.text

def download_dir(userid=None,gkey=None, d_dir=None, dir=''):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if d_dir==None or d_dir=='':
        print("Please enter the path for download directory with the filename to be saved")
        return "Please enter the path for download directory with the filename to be saved"

    if dir=='':
        r=requests.get("https://"+download_IP+":3443/grey/grey_dir/"+gkey+"/"+userid, verify=False)
    else:
        r=requests.get("https://"+download_IP+":3443/grey/grey_dir/"+gkey+"/"+userid+"/"+dir, verify=False)
    

    if "INVALID" in r.text:
        print(r.text)
        return r.text
    else:
        with open(d_dir,'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return 'Downloaded directory succesfully'

def delete_dir(userid=None,gkey=None,dir=None):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    if dir==None:
        print("Please enter the complete path of directory to be deleted")
        return "Please enter the complete path of directory to be deleted"

    if dir=='':
        print("Please enter the directory")
        return "Please enter the directory"
    else:
        r=requests.get("https://"+server_IP+":2443/grey/delete_dir/"+gkey+"/"+userid+"/"+dir, verify=False)
    print(r.text)
    return r.text

def view_user_dir(userid=None,gkey=None):
    if userid==None or userid=='':
        print("Please enter the username")
        return "Please enter the username"
    if gkey==None or gkey=='':
        print("Please enter the greyfish key")
        return "Please enter the greyfish key"
    r=requests.get("https://"+download_IP+":3443/grey/grey_dir_json/"+gkey+"/"+userid, verify=False)
    print(r.text)
    return r.text

    

@app.route("/create_user",methods=["POST"])
def create_user_ui():
    name=request.form['uname']
    key=request.form['gkey']
    msg=add_user(name,key)
    return index('create_user',msg)

@app.route("/delete_user",methods=["POST"])
def remove_user_ui():
    name=request.form['uname']
    key=request.form['gkey']
    msg=remove_user(name,key)
    return index('delete_user',msg)

@app.route("/file_upload",methods=["POST"])
def upload_file_ui():
    name=request.form['uname']
    key=request.form['gkey']
    ucdir=request.form['ucdir']
    file = request.files['file']
    fnam = file.filename
    # Avoids empty filenames and those with commas
    if fnam == '':
       return index('file_upload','INVALID, no file uploaded')
    if ',' in fnam:
       return index('file_upload',"INVALID, no ',' allowed in filenames")
    msg=upload_file(name, key, file, ucdir.replace('/','++'))
    return index('file_upload',msg)

@app.route("/file_download",methods=["GET","POST"])
def download_file_ui():
    name=request.form['uname']
    key=request.form['gkey']
    dwcdir=request.form['dwcdir']
    fnam = request.form['dwfile']
    new_name = secure_filename(fnam)
    msg=''
    try:
        msg=download_file(name, key, new_name, new_name.split('.')[0]+".tar.gz", dwcdir.replace('/','++'))
    except Exception as error:
        print(error)
        msg="Server timed out while downloading. File may be too large."
    if msg != 'File downloaded successfully':
        return index('file_download',msg)
    @after_this_request
    def remove_file(response):
        try:
            os.remove(new_name.split('.')[0]+".tar.gz")
        except Exception as error:
            app.logger.error("Error removing or closing downloaded file handle", error)
        return response

    return send_file(new_name.split('.')[0]+".tar.gz",as_attachment=True)


@app.route("/file_delete",methods=["POST"])
def delete_file_ui():
    name=request.form['uname']
    key=request.form['gkey']
    dfile=request.form['dfile']
    dcdir=request.form['dcdir']
    msg=delete_file(name, key, dfile, dcdir.replace('/','++'))
    return index('file_delete',msg)

@app.route("/directory_upload",methods=["POST"])
def upload_dir_ui():
    name=request.form['uname']
    key=request.form['gkey']
    ucdir=request.form['ucdir']
    file = request.files['file']
    fnam = file.filename
    # Avoids empty filenames and those with commas
    if fnam == '':
        return index('dir_upload','INVALID, no file uploaded')
    if ',' in fnam:
        return index('dir_upload',"INVALID, no ',' allowed in filenames")
    # Untars the file, makes a directory if it does not exist
    if ('.tar.gz' not in fnam) and ('.tgz' not in fnam):
        return index('dir_upload','ERROR: Compression file not accepted, file must be .tgz or .tar.gz')
    if 'replace' in request.form:
        msg=upload_replace_dir(name, key, file, ucdir.replace('/','++'))
    else:
        msg=upload_dir(name, key, file, ucdir.replace('/','++'))
    return index('dir_upload',msg)

@app.route("/directory_download",methods=["GET","POST"])
def download_dir_ui():
    name=request.form['uname']
    key=request.form['gkey']
    dwdir=request.form['dwdir']
    msg=''
    try:
        msg=download_dir(name, key, dwdir.split('/')[-1]+".tar.gz", dwdir.replace('/','++'))
    except Exception as error:
        msg="Server timed out while downloading. Folder may be too large."
    if msg != 'Downloaded directory succesfully':
        return index('dir_download',msg)
    @after_this_request
    def remove_file(response):
        try:
            os.remove(dwdir.split('/')[-1]+".tar.gz")
        except Exception as error:
            app.logger.error("Error removing or closing downloaded file handle", error)
        return response

    return send_file(dwdir.split('/')[-1]+".tar.gz",as_attachment=True)

@app.route("/directory_delete",methods=["POST"])
def delete_dir_ui():
    name=request.form['uname']
    key=request.form['gkey']
    dcdir=request.form['dcdir']
    msg=delete_dir(name, key, dcdir.replace('/','++'))
    return index('dir_delete',msg)

@app.route("/view_data",methods=["POST"])
def view_data_ui():
    name=request.form['uname']
    key=request.form['gkey']
    msg=view_user_dir(name, key)
    return index('view_data',msg)

if __name__ == "__main__":
    app.run()
