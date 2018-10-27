import os

#set paths
inPath = '{}/../../input/'.format(os.getcwd())
outPath = '{}/../../output/'.format(os.getcwd())
sourceDir = '{}maildir/'.format(inPath)


#download the data
url = 'https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz'
enronFile = os.path.basename(url)
os.chdir(inPath)

os.system('wget {}'.format(url))

#unzip the data
os.system('tar -xvzf {}'.format(enronFile))