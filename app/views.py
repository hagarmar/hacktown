from app import app
from flask import Flask, render_template

@app.route('/')
@app.route('/index')
def index():
	return render_template('index.html')

@app.route('/hello')
def hello():
	return render_template('hello.html')