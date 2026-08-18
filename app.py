# ==================== TIMEZONE SETUP ====================
from datetime import datetime, date, timedelta, timezone
import os
import time as time_module
import pytz

# Try to load .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded .env file")
except ImportError:
    print("⚠️ python-dotenv not installed, using system environment variables")

# Cambodia timezone (UTC+7)
CAMBODIA_TZ = pytz.timezone('Asia/Phnom_Penh')


def cambodia_now():
    """Get current datetime in Cambodia timezone"""
    return datetime.now(CAMBODIA_TZ)


def cambodia_today():
    """Get today's date in Cambodia timezone"""
    return cambodia_now().date()


# ==================== APP START TIME ====================
APP_START_TIME = time_module.time()

print(f"🕐 Cambodia Time (UTC+7): {cambodia_now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📅 Cambodia Date: {cambodia_today().strftime('%Y-%m-%d')}")

# ==================== IMPORTS ====================
from flask import Flask, render_template, request, redirect, url_for, session, flash, send_from_directory, jsonify, \
    make_response, Response, g
from flask_sqlalchemy import SQLAlchemy
from functools import wraps
from flask import send_file
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from PIL import Image
import json
from sqlalchemy import inspect, text, not_, and_, or_, func, desc
from sqlalchemy.pool import NullPool
import csv
from io import StringIO
import threading
import queue
import base64
import io
from collections import defaultdict
import hashlib
import glob

# ==================== CLOUDINARY SETUP ====================
USE_CLOUDINARY = False
CLOUDINARY_CONFIGURED = False

if os.environ.get('VERCEL') and os.environ.get('CLOUDINARY_CLOUD_NAME'):
    try:
        import cloudinary
        import cloudinary.uploader
        import cloudinary.api
        from cloudinary.utils import cloudinary_url

        cloudinary.config(
            cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME'),
            api_key=os.environ.get('CLOUDINARY_API_KEY'),
            api_secret=os.environ.get('CLOUDINARY_API_SECRET')
        )
        USE_CLOUDINARY = True
        CLOUDINARY_CONFIGURED = True
        print("✅ Cloudinary configured for permanent profile picture storage")
    except ImportError:
        print("⚠️ Cloudinary not installed - falling back to local storage")
        USE_CLOUDINARY = False
else:
    print("📁 Using local file storage for profile pictures")

# ==================== VERCEL READ-ONLY FIX ====================
if os.environ.get('VERCEL'):
    import tempfile
    temp_dir = tempfile.gettempdir()
    app = Flask(__name__, instance_path=os.path.join(temp_dir, 'instance'))
else:
    app = Flask(__name__)


# ==================== SMART DATABASE URI ====================
def get_database_uri():
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("📁 DATABASE_URL not set – using SQLite")
        return 'sqlite:///database.db'

    if 'sslmode' not in database_url:
        database_url += ('&' if '?' in database_url else '?') + 'sslmode=require'

    if os.environ.get('VERCEL'):
        return database_url

    try:
        from sqlalchemy import create_engine
        engine = create_engine(
            database_url,
            connect_args={'connect_timeout': 5},
            pool_size=1,
            max_overflow=0,
            pool_recycle=300
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"✅ Successfully connected to PostgreSQL")
        return database_url
    except Exception as e:
        print(f"⚠️ PostgreSQL connection failed: {e}")
        print("📁 Falling back to SQLite")
        return 'sqlite:///database.db'


# ==================== CONFIGURATION ====================
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(16))

database_uri = get_database_uri()
app.config['SQLALCHEMY_DATABASE_URI'] = database_uri
print(f"🔗 Final Database URI: {app.config['SQLALCHEMY_DATABASE_URI']}")

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# ==================== OPTIMIZED DATABASE CONFIG ====================
if os.environ.get('VERCEL'):
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'poolclass': NullPool,
        'connect_args': {'connect_timeout': 5},
        'echo': False
    }
else:
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_size': 10,
        'max_overflow': 20,
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'pool_timeout': 30,
        'connect_args': {'connect_timeout': 5},
        'echo': False
    }

if os.environ.get('VERCEL'):
    app.config['UPLOAD_FOLDER'] = '/tmp/uploads/profiles'
else:
    app.config['UPLOAD_FOLDER'] = 'static/uploads/profiles'

app.config['MAX_CONTENT_LENGTH'] = 500 * 1024  # 500KB max
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif'}
app.config['MAX_IMAGE_SIZE'] = 150
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

db = SQLAlchemy(app)

# SSE clients for real-time updates
sse_clients = {}
user_heartbeats = {}

# ==================== IMPROVED CACHE SYSTEM ====================
cache_data = {}
cache_timestamps = {}


def cache_get(key):
    """Get cached data with faster lookup"""
    if key in cache_data:
        if time_module.time() - cache_timestamps.get(key, 0) < 120:
            return cache_data[key]
        else:
            cache_delete(key)
    return None


def cache_set(key, value, timeout=120):
    """Set cached data"""
    cache_data[key] = value
    cache_timestamps[key] = time_module.time()


def cache_delete(key):
    """Delete cached data"""
    cache_data.pop(key, None)
    cache_timestamps.pop(key, None)


def cache_clear():
    """Clear all cache"""
    cache_data.clear()
    cache_timestamps.clear()


# ==================== MODELS ====================

class Member(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    full_name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=True)
    phone = db.Column(db.String(20), nullable=True)
    is_admin = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True)
    is_online = db.Column(db.Boolean, default=False)
    profile_picture = db.Column(db.String(500), default='default.png')
    created_at = db.Column(db.DateTime(timezone=True), default=cambodia_now)
    last_login = db.Column(db.DateTime(timezone=True), nullable=True)

    records = db.relationship('CollectionRecord', backref='member', lazy='select', cascade='all, delete-orphan')
    logs = db.relationship('SystemLog', backref='user', lazy='select')
    password_resets = db.relationship(
        'PasswordResetRequest',
        backref='member',
        lazy='select',
        cascade='all, delete-orphan'
    )

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class CollectionRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    member_id = db.Column(db.Integer, db.ForeignKey('member.id'), nullable=False)
    date = db.Column(db.Date, nullable=False, default=cambodia_today)
    call_time = db.Column(db.String(20), nullable=True)
    receive_time = db.Column(db.String(20), nullable=True)
    duration = db.Column(db.String(20), nullable=True)
    total_sampling = db.Column(db.Integer, nullable=False, default=0)
    location = db.Column(db.String(200), nullable=True)
    sample_types_json = db.Column(db.Text, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), default=cambodia_now)
    updated_at = db.Column(db.DateTime(timezone=True), default=cambodia_now, onupdate=cambodia_now)
    patient_count = db.Column(db.Integer, nullable=False, default=1)
    doctor = db.Column(db.String(100), nullable=True)
    moto_fee = db.Column(db.Integer, nullable=False, default=1000)
    far_away_fee = db.Column(db.Integer, nullable=False, default=5000)


class SystemLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('member.id'), nullable=True)
    action = db.Column(db.String(200), nullable=False)
    details = db.Column(db.Text, nullable=True)
    timestamp = db.Column(db.DateTime(timezone=True), default=cambodia_now)


class PasswordResetRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    member_id = db.Column(db.Integer, db.ForeignKey('member.id'), nullable=True)
    fullname = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(50), nullable=False)
    phone = db.Column(db.String(20), nullable=True)
    request_code = db.Column(db.String(50), nullable=True)
    reason = db.Column(db.Text, nullable=True)
    request_date = db.Column(db.DateTime(timezone=True), default=cambodia_now)
    status = db.Column(db.String(20), default='Pending')
    admin_note = db.Column(db.Text, nullable=True)

    def __repr__(self):
        return f'<PasswordResetRequest {self.username} - {self.status}>'


# ==================== CONSTANTS ====================
SAMPLE_TYPES = [
    "Blood", "Serum", "Plasma", "Urine", "Stool",
    "Sputum", "Swab", "Saliva", "CSF", "Pleural Fluid",
    "Ascitic Fluid", "Synovial Fluid", "Pericardial Fluid",
    "Amniotic Fluid", "Semen", "Tissue / Biopsy",
    "Bone Marrow", "Dried Blood Spot (DBS)",
    "Newborn Screening (NBS)", "Cytology",
    "Fine Needle Aspiration (FNA)", "Other"
]

EXCLUDE_FEE_ONLY = not_(and_(
    CollectionRecord.patient_count == 0,
    CollectionRecord.total_sampling == 0
))


# ==================== HELPERS ====================
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def save_profile_picture(file, member_id):
    """Save profile picture with Cloudinary support for permanent storage"""
    if USE_CLOUDINARY and file and allowed_file(file.filename):
        try:
            upload_result = cloudinary.uploader.upload(
                file,
                folder=f"cammedlab/profiles/{member_id}",
                transformation=[
                    {'width': 150, 'height': 150, 'crop': 'thumb', 'gravity': 'face'},
                    {'quality': 'auto:low'},
                    {'fetch_format': 'auto'}
                ],
                public_id=f"member_{member_id}_{secrets.token_hex(8)}"
            )
            permanent_url = upload_result.get('secure_url')
            print(f"✅ Profile picture uploaded to Cloudinary: {permanent_url}")
            return permanent_url
        except Exception as e:
            print(f"❌ Cloudinary upload error: {e}")
            return 'default.png'

    if file and allowed_file(file.filename):
        ext = file.filename.rsplit('.', 1)[1].lower()
        filename = f"member_{member_id}_{int(time_module.time())}_{secrets.token_hex(8)}.{ext}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        try:
            img = Image.open(file)
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            max_size = app.config.get('MAX_IMAGE_SIZE', 150)
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

            if ext.lower() in ('jpg', 'jpeg'):
                img.save(filepath, 'JPEG', quality=60, optimize=True)
            elif ext.lower() == 'png':
                img.save(filepath, 'PNG', optimize=True)
            else:
                img.save(filepath, optimize=True)
            print(f"✅ Profile picture saved locally: {filename}")
            return filename
        except Exception as e:
            print(f"❌ Error saving profile picture: {e}")
            return 'default.png'
    return 'default.png'


def process_base64_image_to_cloudinary(image_data, member_id):
    """Process base64 image and upload to Cloudinary for permanent storage"""
    if not USE_CLOUDINARY:
        return None

    try:
        header, encoded = image_data.split(',', 1)
        image_bytes = base64.b64decode(encoded)

        from io import BytesIO
        upload_result = cloudinary.uploader.upload(
            BytesIO(image_bytes),
            folder=f"cammedlab/profiles/{member_id}",
            transformation=[
                {'width': 150, 'height': 150, 'crop': 'thumb', 'gravity': 'face'},
                {'quality': 'auto:low'},
                {'fetch_format': 'auto'}
            ],
            public_id=f"member_{member_id}_{secrets.token_hex(8)}"
        )
        return upload_result.get('secure_url')
    except Exception as e:
        print(f"❌ Cloudinary upload error: {e}")
        return None


def process_base64_image_locally(image_data, member_id):
    """Process base64 image and save locally"""
    try:
        header, encoded = image_data.split(',', 1)
        image_bytes = base64.b64decode(encoded)

        ext = 'png' if 'png' in header else 'jpg'
        filename = f"member_{member_id}_{int(time_module.time())}_{secrets.token_hex(8)}.{ext}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        img = Image.open(io.BytesIO(image_bytes))
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')

        max_size = app.config.get('MAX_IMAGE_SIZE', 150)
        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

        if ext == 'png':
            img.save(filepath, 'PNG', optimize=True)
        else:
            img.save(filepath, 'JPEG', quality=60, optimize=True)
        return filename
    except Exception as e:
        print(f"❌ Error processing image locally: {e}")
        return None


def delete_old_profile_picture(profile_picture, member_id):
    """Delete old profile picture from storage"""
    if not profile_picture or profile_picture == 'default.png':
        return

    if USE_CLOUDINARY and profile_picture.startswith('http'):
        return

    old = os.path.join(app.config['UPLOAD_FOLDER'], profile_picture)
    if os.path.exists(old):
        try:
            os.remove(old)
            print(f"✅ Deleted old profile picture: {profile_picture}")
        except Exception as e:
            print(f"⚠️ Could not delete old profile picture: {e}")


def parse_time(time_str):
    if not time_str:
        return None
    time_str = time_str.strip()
    formats = ['%I:%M %p', '%I %p', '%I:%M%p', '%I%p']
    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt)
        except:
            continue
    return None


def calculate_duration(call_time, receive_time):
    call_dt = parse_time(call_time)
    receive_dt = parse_time(receive_time)
    if call_dt and receive_dt:
        diff = receive_dt - call_dt
        if diff.total_seconds() < 0:
            return None
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        if hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    return None


def get_sample_types_display(sample_types_json):
    if not sample_types_json:
        return '-'
    try:
        data = json.loads(sample_types_json)
        parts = []
        for stype, count in data.items():
            if count > 0:
                parts.append(f"{count} {stype}")
        return ', '.join(parts) if parts else '-'
    except:
        return '-'


def add_system_log(user_id, action, details=None):
    try:
        log = SystemLog(user_id=user_id, action=action, details=details)
        db.session.add(log)
        db.session.commit()
    except Exception as e:
        print(f"❌ LOG ERROR: {e}")
        db.session.rollback()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please login first.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please login first.', 'warning')
            return redirect(url_for('login'))
        if not session.get('is_admin'):
            flash('Admin access required.', 'danger')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function


def broadcast_status_update(user_id, is_online, user_data=None):
    """Send status update to all SSE clients"""
    if user_data is None:
        user_data = {}
    data = {
        'type': 'status_update',
        'user_id': user_id,
        'is_online': is_online,
        'user_data': user_data
    }
    for client_id, client_queue in list(sse_clients.items()):
        try:
            client_queue.put(json.dumps(data))
        except:
            pass


# ==================== TEMPLATE FILTERS ====================
@app.template_filter('cambodia_time')
def cambodia_time_filter(dt):
    if dt:
        try:
            if dt.tzinfo is None:
                cambodia_dt = dt.replace(tzinfo=CAMBODIA_TZ)
            else:
                cambodia_dt = dt.astimezone(CAMBODIA_TZ)
            return cambodia_dt.strftime('%Y-%m-%d %H:%M')
        except Exception:
            return dt.strftime('%Y-%m-%d %H:%M')
    return '-'


@app.template_filter('cambodia_date')
def cambodia_date_filter(dt):
    if dt:
        try:
            if dt.tzinfo is None:
                cambodia_dt = dt.replace(tzinfo=CAMBODIA_TZ)
            else:
                cambodia_dt = dt.astimezone(CAMBODIA_TZ)
            return cambodia_dt.strftime('%Y-%m-%d')
        except Exception:
            return dt.strftime('%Y-%m-%d')
    return '-'


@app.template_filter('sample_types_display')
def sample_types_display_filter(sample_types_json):
    return get_sample_types_display(sample_types_json)


@app.template_filter('profile_picture_url')
def profile_picture_url_filter(member):
    """Return the correct URL for a profile picture"""
    if not member or not member.profile_picture:
        return url_for('uploaded_file', filename='default.png')
    if member.profile_picture.startswith('http'):
        return member.profile_picture
    return url_for('uploaded_file', filename=member.profile_picture)


@app.template_filter('profile_url')
def profile_url_filter(profile_picture):
    """Return the correct URL for a profile picture"""
    if not profile_picture:
        return url_for('uploaded_file', filename='default.png')
    if profile_picture.startswith('http'):
        return profile_picture
    return url_for('uploaded_file', filename=profile_picture)


@app.template_filter('profile_image_url')
def profile_image_url_filter(profile_picture):
    """Return the correct URL for a profile picture"""
    if not profile_picture or profile_picture == 'default.png':
        return url_for('uploaded_file', filename='default.png')
    if profile_picture.startswith('http://') or profile_picture.startswith('https://'):
        return profile_picture
    return url_for('uploaded_file', filename=profile_picture)


# ==================== SSE STREAM ====================
@app.route('/stream')
def stream():
    def event_stream():
        client_id = secrets.token_hex(8)
        q = queue.Queue()
        sse_clients[client_id] = q
        try:
            while True:
                data = q.get()
                yield f"data: {data}\n\n"
        except GeneratorExit:
            del sse_clients[client_id]

    return Response(event_stream(), mimetype="text/event-stream")


# ==================== QUERY PROFILING ====================
@app.before_request
def before_request():
    if 'user_id' in session:
        try:
            member = db.session.get(Member, session['user_id'])
            if member and member.is_active:
                was_offline = not member.is_online
                now = cambodia_now()
                member.is_online = True
                member.last_login = now
                db.session.commit()

                if was_offline:
                    broadcast_status_update(member.id, True, {
                        'full_name': member.full_name,
                        'username': member.username,
                        'profile_picture': member.profile_picture
                    })
        except:
            db.session.rollback()

    if app.debug:
        g.start_time = time_module.time()


@app.after_request
def after_request(response):
    if app.debug and hasattr(g, 'start_time'):
        elapsed = time_module.time() - g.start_time
        if elapsed > 0.5:
            print(f"⚠️ SLOW REQUEST: {request.path} took {elapsed:.2f}s")
            try:
                from flask_sqlalchemy import get_debug_queries
                for query in get_debug_queries():
                    if query.duration > 0.1:
                        print(f"  Query: {query.statement[:200]}... ({query.duration:.3f}s)")
            except:
                pass
    return response


# ==================== ROUTES ====================

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        if request.form.get('action') == 'reset_password':
            fullname = request.form.get('fullname', '').strip()
            username = request.form.get('username', '').strip()
            phone = request.form.get('phone', '').strip()
            code = request.form.get('code', '').strip()
            reason = request.form.get('reason', '').strip()

            if not fullname:
                flash('❌ Please enter your full name.', 'danger')
                return render_template('login.html', show_reset_form=True)
            if not username:
                flash('❌ Please enter your username.', 'danger')
                return render_template('login.html', show_reset_form=True)
            if not phone:
                flash('❌ Please enter your phone number.', 'danger')
                return render_template('login.html', show_reset_form=True)
            if not code:
                flash('❌ Please enter your request code.', 'danger')
                return render_template('login.html', show_reset_form=True)
            if not reason or len(reason) < 10:
                flash('❌ Please provide a detailed reason (minimum 10 characters).', 'danger')
                return render_template('login.html', show_reset_form=True)

            member = Member.query.filter_by(username=username).first()
            if not member:
                flash('❌ No account found with this username.', 'danger')
                return render_template('login.html', show_reset_form=True)

            phone_input = ''.join(filter(str.isdigit, phone))
            phone_db = ''.join(filter(str.isdigit, member.phone)) if member.phone else ''
            if not phone_db or phone_input != phone_db:
                flash('❌ The phone number does not match our records. Please check and try again.', 'danger')
                return render_template('login.html', show_reset_form=True)

            username_matches = (member.username.lower() == username.lower())
            name_matches = (member.full_name.lower() == fullname.lower())
            if not username_matches and not name_matches:
                flash('❌ Neither username nor full name match our records.', 'danger')
                return render_template('login.html', show_reset_form=True)

            existing = PasswordResetRequest.query.filter(
                PasswordResetRequest.request_code == code,
                PasswordResetRequest.status.in_(['Pending', 'Approved'])
            ).first()
            if existing:
                flash('❌ This request code is already in use (pending or approved). If rejected, you can reuse it.',
                      'danger')
                return render_template('login.html', show_reset_form=True)

            try:
                reset_request = PasswordResetRequest(
                    member_id=member.id,
                    fullname=fullname,
                    username=username,
                    phone=phone if phone else None,
                    request_code=code,
                    reason=reason,
                    status='Pending'
                )
                db.session.add(reset_request)
                db.session.commit()
                add_system_log(None, 'Password Reset Request',
                               f'User {username} requested password reset (Code: {code})')
                flash('✅ Password reset request submitted. Please wait for admin approval.', 'success')
                return render_template('login.html')
            except Exception as e:
                flash(f'❌ Database error: {str(e)}', 'danger')
                return render_template('login.html', show_reset_form=True)

        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if not username:
            flash('❌ Please enter a username.', 'danger')
            return render_template('login.html')

        try:
            member = Member.query.filter_by(username=username).first()
            if member and member.check_password(password) and member.is_active:
                session.clear()
                session['user_id'] = member.id
                session['username'] = member.username
                session['full_name'] = member.full_name
                session['is_admin'] = member.is_admin
                session['profile_picture'] = member.profile_picture
                session['profile_version'] = 1

                now = cambodia_now()
                member.last_login = now
                member.is_online = True
                db.session.commit()

                broadcast_status_update(member.id, True, {
                    'full_name': member.full_name,
                    'username': member.username,
                    'profile_picture': member.profile_picture
                })

                add_system_log(member.id, 'Login', f'Collector {member.username} logged in')
                flash(f'Welcome, {member.full_name}! 👋', 'success')
                return redirect(url_for('dashboard'))
            else:
                flash('❌ Invalid username or password.', 'danger')
        except Exception as e:
            flash(f'❌ Database error: {str(e)}', 'danger')

    return render_template('login.html')


@app.route('/logout')
def logout():
    if 'user_id' in session:
        member = db.session.get(Member, session['user_id'])
        if member:
            member.is_online = False
            member.last_login = cambodia_now()
            db.session.commit()

            broadcast_status_update(member.id, False, {
                'full_name': member.full_name,
                'username': member.username
            })

            add_system_log(session['user_id'], 'Logout', f'Collector {member.username} logged out')
    session.clear()
    flash('Logged out successfully.', 'info')
    return redirect(url_for('login'))


@app.route('/set_offline')
@login_required
def set_offline():
    """API endpoint to mark user as offline when they close the tab/browser"""
    if 'user_id' in session:
        member = db.session.get(Member, session['user_id'])
        if member and member.is_online:
            member.is_online = False
            db.session.commit()
            broadcast_status_update(member.id, False, {
                'full_name': member.full_name,
                'username': member.username
            })
            return jsonify({'status': 'offline'})
    return jsonify({'status': 'error'}), 400


@app.route('/check_online_status')
@login_required
def check_online_status():
    member = db.session.get(Member, session['user_id'])
    if member:
        was_offline = not member.is_online
        now = cambodia_now()
        member.is_online = True
        member.last_login = now
        db.session.commit()

        if was_offline:
            broadcast_status_update(member.id, True, {
                'full_name': member.full_name,
                'username': member.username,
                'profile_picture': member.profile_picture
            })

        return jsonify({
            'status': 'ok',
            'is_online': member.is_online
        })
    return jsonify({'status': 'error'}), 404


@app.route('/api/status/<int:user_id>')
def get_user_status(user_id):
    member = db.session.get(Member, user_id)
    if member:
        return jsonify({
            'id': member.id,
            'full_name': member.full_name,
            'username': member.username,
            'is_online': member.is_online,
            'profile_picture': member.profile_picture
        })
    return jsonify({'error': 'User not found'}), 404


@app.route('/api/bulk_status')
def get_bulk_status():
    user_ids = request.args.get('ids', '').split(',')
    if not user_ids or not user_ids[0]:
        return jsonify([])

    members = Member.query.filter(Member.id.in_([int(id) for id in user_ids if id.isdigit()])).all()
    result = []
    for m in members:
        result.append({
            'id': m.id,
            'full_name': m.full_name,
            'username': m.username,
            'is_online': m.is_online,
            'profile_picture': m.profile_picture
        })
    return jsonify(result)


@app.route('/api/ping')
@login_required
def ping():
    """Keep user online with heartbeat"""
    member = db.session.get(Member, session['user_id'])
    if member:
        was_offline = not member.is_online
        now = cambodia_now()
        member.is_online = True
        member.last_login = now
        db.session.commit()

        if was_offline:
            broadcast_status_update(member.id, True, {
                'full_name': member.full_name,
                'username': member.username,
                'profile_picture': member.profile_picture
            })

        return jsonify({'status': 'ok'})
    return jsonify({'status': 'error'}), 404


# ==================== OPTIMIZED DASHBOARD ====================
@app.route('/dashboard')
@login_required
def dashboard():
    member = db.session.get(Member, session['user_id'])
    if not member or not member.is_active:
        session.clear()
        flash('Account is deactivated.', 'danger')
        return redirect(url_for('login'))

    today = cambodia_today()
    user_id = session['user_id']

    cache_key = f'dashboard_{user_id}_{today.strftime("%Y-%m-%d")}'
    dashboard_data = cache_get(cache_key)

    if dashboard_data is None:
        # Single query for all today's aggregates
        today_aggregates = db.session.query(
            func.count(CollectionRecord.id).label('count'),
            func.sum(CollectionRecord.total_sampling).label('total_sampling'),
            func.sum(CollectionRecord.patient_count).label('total_patients'),
            func.sum(CollectionRecord.moto_fee).label('total_moto'),
            func.sum(CollectionRecord.far_away_fee).label('total_far')
        ).filter(
            CollectionRecord.member_id == user_id,
            CollectionRecord.date == today,
            EXCLUDE_FEE_ONLY
        ).first()

        # Get today's records with limit
        today_records = CollectionRecord.query.filter(
            CollectionRecord.member_id == user_id,
            CollectionRecord.date == today,
            EXCLUDE_FEE_ONLY
        ).order_by(CollectionRecord.call_time.asc()).limit(100).all()

        # Last 7 days using a single query with GROUP BY
        week_ago = today - timedelta(days=6)
        weekly_data = db.session.query(
            CollectionRecord.date,
            func.sum(CollectionRecord.total_sampling).label('total_sampling'),
            func.count(CollectionRecord.id).label('count')
        ).filter(
            CollectionRecord.member_id == user_id,
            CollectionRecord.date >= week_ago,
            CollectionRecord.date <= today,
            EXCLUDE_FEE_ONLY
        ).group_by(CollectionRecord.date).all()

        weekly_dict = {d.date: d for d in weekly_data}
        last_7_days = []
        week_total = 0
        week_entries = 0

        for i in range(6, -1, -1):
            day = today - timedelta(days=i)
            day_data = weekly_dict.get(day)
            day_sampling = day_data.total_sampling if day_data else 0
            day_entries = day_data.count if day_data else 0
            week_total += day_sampling
            week_entries += day_entries
            last_7_days.append({
                'date': day.strftime('%Y-%m-%d'),
                'day_name': day.strftime('%a'),
                'sampling': day_sampling,
                'entries': day_entries
            })

        # Calculate week metrics
        week_max = max((day['sampling'] for day in last_7_days), default=0)
        week_avg = round(week_total / 7, 1) if last_7_days else 0

        # Total aggregates using a single query
        total_aggregates = db.session.query(
            func.count(CollectionRecord.id).label('count'),
            func.sum(CollectionRecord.total_sampling).label('total_sampling'),
            func.sum(CollectionRecord.patient_count).label('total_patients')
        ).filter(
            CollectionRecord.member_id == user_id,
            EXCLUDE_FEE_ONLY
        ).first()

        dashboard_data = {
            'today_records': today_records,
            'today_sampling': today_aggregates.total_sampling or 0,
            'today_patients': today_aggregates.total_patients or 0,
            'today_entries_count': today_aggregates.count or 0,
            'today_moto_fee': today_aggregates.total_moto or 0,
            'today_far_away_fee': today_aggregates.total_far or 0,
            'today_total_fees': (today_aggregates.total_moto or 0) + (today_aggregates.total_far or 0),
            'last_7_days': last_7_days,
            'week_max': week_max,
            'week_total': week_total,
            'week_entries': week_entries,
            'week_avg': week_avg,
            'total_records': total_aggregates.count or 0,
            'total_all_sampling': total_aggregates.total_sampling or 0,
            'total_patients': total_aggregates.total_patients or 0
        }

        cache_set(cache_key, dashboard_data, timeout=120)

    period = request.args.get('period', 'monthly')
    from_date = request.args.get('from_date', '').strip()
    to_date = request.args.get('to_date', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 50

    query = CollectionRecord.query.filter_by(member_id=user_id).filter(EXCLUDE_FEE_ONLY)

    # Store filter conditions for aggregate query
    filter_conditions = []

    if from_date and to_date:
        try:
            from_dt = datetime.strptime(from_date, '%Y-%m-%d').date()
            to_dt = datetime.strptime(to_date, '%Y-%m-%d').date()
            query = query.filter(CollectionRecord.date >= from_dt, CollectionRecord.date <= to_dt)
            filter_conditions.append(CollectionRecord.date >= from_dt)
            filter_conditions.append(CollectionRecord.date <= to_dt)
            period_label = f"Custom ({from_date} → {to_date})"
        except ValueError:
            flash('Invalid date format. Use YYYY-MM-DD.', 'danger')
            return redirect(url_for('dashboard', period='monthly'))
    else:
        if period == 'daily':
            query = query.filter(CollectionRecord.date == today)
            filter_conditions.append(CollectionRecord.date == today)
            period_label = "Daily"
        elif period == 'weekly':
            start_week = today - timedelta(days=today.weekday())
            end_week = start_week + timedelta(days=6)
            query = query.filter(CollectionRecord.date >= start_week, CollectionRecord.date <= end_week)
            filter_conditions.append(CollectionRecord.date >= start_week)
            filter_conditions.append(CollectionRecord.date <= end_week)
            period_label = f"Weekly ({start_week.strftime('%Y-%m-%d')} → {end_week.strftime('%Y-%m-%d')})"
        elif period == 'yearly':
            query = query.filter(db.extract('year', CollectionRecord.date) == today.year)
            filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)
            period_label = f"Yearly ({today.year})"
        else:
            query = query.filter(
                db.extract('month', CollectionRecord.date) == today.month,
                db.extract('year', CollectionRecord.date) == today.year
            )
            filter_conditions.append(db.extract('month', CollectionRecord.date) == today.month)
            filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)
            period_label = f"Monthly ({today.strftime('%B %Y')})"

    paginated = query.order_by(
        CollectionRecord.date.desc(),
        CollectionRecord.call_time.asc()
    ).paginate(page=page, per_page=per_page, error_out=False)

    filtered_records = paginated.items

    # Build aggregate query with the same filters
    agg_query = db.session.query(
        func.count(CollectionRecord.id).label('count'),
        func.sum(CollectionRecord.total_sampling).label('total_sampling'),
        func.sum(CollectionRecord.patient_count).label('total_patients'),
        func.sum(func.coalesce(CollectionRecord.moto_fee, 0) + func.coalesce(CollectionRecord.far_away_fee, 0)).label('total_fees')
    ).filter(
        CollectionRecord.member_id == user_id,
        EXCLUDE_FEE_ONLY
    )

    for condition in filter_conditions:
        agg_query = agg_query.filter(condition)

    filtered_aggregates = agg_query.first()

    filtered_total_records = filtered_aggregates.count or 0 if filtered_aggregates else 0
    filtered_total_sampling = filtered_aggregates.total_sampling or 0 if filtered_aggregates else 0
    filtered_total_patients = filtered_aggregates.total_patients or 0 if filtered_aggregates else 0
    filtered_total_fees = filtered_aggregates.total_fees or 0 if filtered_aggregates else 0
    filtered_avg = round(filtered_total_sampling / filtered_total_records, 2) if filtered_total_records else 0

    return render_template('dashboard.html',
                           member=member, today=today,
                           **dashboard_data,
                           filtered_records=filtered_records,
                           filtered_total_records=filtered_total_records,
                           filtered_total_sampling=filtered_total_sampling,
                           filtered_total_patients=filtered_total_patients,
                           filtered_total_fees=filtered_total_fees,
                           filtered_avg=filtered_avg,
                           period=period,
                           period_label=period_label,
                           from_date=from_date,
                           to_date=to_date,
                           page=page,
                           total_pages=paginated.pages if paginated else 1,
                           get_sample_types_display=get_sample_types_display)


# ==================== ADD RECORD ====================
@app.route('/add_record', methods=['GET', 'POST'])
@login_required
def add_record():
    if request.method == 'POST':
        collection_date = request.form.get('collection_date', '').strip()
        call_time = request.form.get('sample_call_time', '').strip()
        receive_time = request.form.get('sample_received_time', '').strip()
        location = request.form.get('location', '').strip()
        doctor = request.form.get('doctor', '').strip()
        sample_count = request.form.get('sample_count', type=int)
        total_specimens = request.form.get('total_specimens', type=int)
        notes = request.form.get('notes', '').strip()

        moto_fee = request.form.get('moto_fee', type=int)
        if moto_fee is None or moto_fee < 0:
            moto_fee = 0
        far_away_fee = request.form.get('far_away_fee', type=int)
        if far_away_fee is None or far_away_fee < 0:
            far_away_fee = 0

        sample_count = sample_count if sample_count is not None else 0
        total_specimens = total_specimens if total_specimens is not None else 0

        sample_data = {}
        for stype in SAMPLE_TYPES:
            count_str = request.form.get(f'sample_{stype}', '0')
            try:
                count = int(count_str) if count_str else 0
            except:
                count = 0
            if count > 0:
                sample_data[stype] = count

        errors = []
        if not collection_date:
            errors.append("Collection date is required.")
        if not call_time:
            errors.append("Sample call time is required.")
        if not receive_time:
            errors.append("Sample received time is required.")
        if not location:
            errors.append("Clinic / Hospital is required.")

        no_collection = (sample_count == 0 and total_specimens == 0)
        if no_collection:
            if not notes:
                errors.append("Patients and Total Specimens are both 0 — please explain why in the Notes field.")
        elif (sample_count == 0) != (total_specimens == 0):
            errors.append(
                "Please enter both Number of Patients and Total Specimens, or leave both at 0 and explain why in the Notes field.")

        if call_time and receive_time:
            try:
                call_dt = datetime.strptime(call_time, '%H:%M').time()
                recv_dt = datetime.strptime(receive_time, '%H:%M').time()
                if recv_dt <= call_dt:
                    errors.append("Received time must be later than call time.")
            except ValueError:
                errors.append("Invalid time format. Use HH:MM.")

        if sample_count > 0 and total_specimens > 0 and total_specimens < sample_count:
            errors.append(f"Total specimens ({total_specimens}) cannot be less than patients ({sample_count}).")

        type_sum = sum(sample_data.values())
        if type_sum > 0 and type_sum != total_specimens:
            errors.append(f"Sum of specimen types ({type_sum}) does not match total specimens ({total_specimens}).")

        if errors:
            for err in errors:
                flash(err, 'danger')
            return render_template('add_record.html', request=request)

        call_time_formatted = None
        if call_time:
            try:
                hour, minute = call_time.split(':')
                hour_int = int(hour)
                ampm = 'PM' if hour_int >= 12 else 'AM'
                if hour_int > 12:
                    hour_int -= 12
                elif hour_int == 0:
                    hour_int = 12
                call_time_formatted = f"{hour_int}:{minute} {ampm}"
            except:
                call_time_formatted = call_time

        receive_time_formatted = None
        if receive_time:
            try:
                hour, minute = receive_time.split(':')
                hour_int = int(hour)
                ampm = 'PM' if hour_int >= 12 else 'AM'
                if hour_int > 12:
                    hour_int -= 12
                elif hour_int == 0:
                    hour_int = 12
                receive_time_formatted = f"{hour_int}:{minute} {ampm}"
            except:
                receive_time_formatted = receive_time

        duration = None
        if call_time_formatted and receive_time_formatted:
            duration = calculate_duration(call_time_formatted, receive_time_formatted)

        try:
            record_date = datetime.strptime(collection_date, '%Y-%m-%d').date() if collection_date else cambodia_today()

            record = CollectionRecord(
                member_id=session['user_id'],
                date=record_date,
                call_time=call_time_formatted,
                receive_time=receive_time_formatted,
                duration=duration,
                total_sampling=total_specimens,
                patient_count=sample_count,
                doctor=doctor if doctor else None,
                location=location if location else None,
                sample_types_json=json.dumps(sample_data) if sample_data else None,
                notes=notes if notes else None,
                moto_fee=moto_fee,
                far_away_fee=far_away_fee
            )

            db.session.add(record)
            db.session.commit()

            cache_delete(f'dashboard_{session["user_id"]}_{cambodia_today().strftime("%Y-%m-%d")}')

            member = db.session.get(Member, session['user_id'])
            add_system_log(session['user_id'], 'Create sampling',
                           f'{member.username} added record: {total_specimens} specimens, {sample_count} patients, fees: moto={moto_fee}, far={far_away_fee}')

            flash(f'✅ Record saved! {total_specimens} specimen(s) from {sample_count} patient(s).', 'success')
            return redirect(url_for('dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Database error: {str(e)}', 'danger')
            return render_template('add_record.html', request=request)

    return render_template('add_record.html', today=cambodia_today(), sample_types=SAMPLE_TYPES)


@app.route('/edit_record/<int:record_id>', methods=['GET', 'POST'])
@login_required
def edit_record(record_id):
    record = CollectionRecord.query.get_or_404(record_id)
    if record.member_id != session['user_id'] and not session.get('is_admin'):
        flash('Unauthorized.', 'danger')
        return redirect(url_for('dashboard'))

    existing_samples = {}
    if record.sample_types_json:
        try:
            existing_samples = json.loads(record.sample_types_json)
        except:
            pass

    call_time_value = ''
    if record.call_time:
        try:
            dt = parse_time(record.call_time)
            if dt:
                call_time_value = dt.strftime('%H:%M')
        except:
            pass

    receive_time_value = ''
    if record.receive_time:
        try:
            dt = parse_time(record.receive_time)
            if dt:
                receive_time_value = dt.strftime('%H:%M')
        except:
            pass

    if request.method == 'POST':
        collection_date = request.form.get('collection_date', '').strip()
        call_time = request.form.get('sample_call_time', '').strip()
        receive_time = request.form.get('sample_received_time', '').strip()
        location = request.form.get('location', '').strip()
        doctor = request.form.get('doctor', '').strip()
        sample_count = request.form.get('sample_count', type=int)
        total_specimens = request.form.get('total_specimens', type=int)
        notes = request.form.get('notes', '').strip()

        moto_fee = request.form.get('moto_fee', type=int)
        if moto_fee is None or moto_fee < 0:
            moto_fee = 0
        far_away_fee = request.form.get('far_away_fee', type=int)
        if far_away_fee is None or far_away_fee < 0:
            far_away_fee = 0

        sample_count = sample_count if sample_count is not None else 0
        total_specimens = total_specimens if total_specimens is not None else 0

        sample_data = {}
        for stype in SAMPLE_TYPES:
            count_str = request.form.get(f'sample_{stype}', '0')
            try:
                count = int(count_str) if count_str else 0
            except:
                count = 0
            if count > 0:
                sample_data[stype] = count

        errors = []
        if not collection_date:
            errors.append("Collection date is required.")
        if not call_time:
            errors.append("Sample call time is required.")
        if not receive_time:
            errors.append("Sample received time is required.")
        if not location:
            errors.append("Clinic / Hospital is required.")

        no_collection = (sample_count == 0 and total_specimens == 0)
        if no_collection:
            if not notes:
                errors.append("Patients and Total Specimens are both 0 — please explain why in the Notes field.")
        elif (sample_count == 0) != (total_specimens == 0):
            errors.append(
                "Please enter both Number of Patients and Total Specimens, or leave both at 0 and explain why in the Notes field.")

        if call_time and receive_time:
            try:
                call_dt = datetime.strptime(call_time, '%H:%M').time()
                recv_dt = datetime.strptime(receive_time, '%H:%M').time()
                if recv_dt <= call_dt:
                    errors.append("Received time must be later than call time.")
            except ValueError:
                errors.append("Invalid time format. Use HH:MM.")

        if sample_count > 0 and total_specimens > 0 and total_specimens < sample_count:
            errors.append(f"Total specimens ({total_specimens}) cannot be less than patients ({sample_count}).")

        type_sum = sum(sample_data.values())
        if type_sum > 0 and type_sum != total_specimens:
            errors.append(f"Sum of specimen types ({type_sum}) does not match total specimens ({total_specimens}).")

        if errors:
            for err in errors:
                flash(err, 'danger')
            return render_template('edit_record.html',
                                   record=record,
                                   call_time_value=call_time,
                                   receive_time_value=receive_time,
                                   existing_samples=existing_samples,
                                   sample_types=SAMPLE_TYPES,
                                   request=request)

        call_time_formatted = None
        if call_time:
            try:
                hour, minute = call_time.split(':')
                hour_int = int(hour)
                ampm = 'PM' if hour_int >= 12 else 'AM'
                if hour_int > 12:
                    hour_int -= 12
                elif hour_int == 0:
                    hour_int = 12
                call_time_formatted = f"{hour_int}:{minute} {ampm}"
            except:
                call_time_formatted = call_time

        receive_time_formatted = None
        if receive_time:
            try:
                hour, minute = receive_time.split(':')
                hour_int = int(hour)
                ampm = 'PM' if hour_int >= 12 else 'AM'
                if hour_int > 12:
                    hour_int -= 12
                elif hour_int == 0:
                    hour_int = 12
                receive_time_formatted = f"{hour_int}:{minute} {ampm}"
            except:
                receive_time_formatted = receive_time

        duration = None
        if call_time_formatted and receive_time_formatted:
            duration = calculate_duration(call_time_formatted, receive_time_formatted)

        try:
            record.date = datetime.strptime(collection_date, '%Y-%m-%d').date() if collection_date else record.date
            record.call_time = call_time_formatted
            record.receive_time = receive_time_formatted
            record.duration = duration
            record.total_sampling = total_specimens
            record.patient_count = sample_count
            record.doctor = doctor if doctor else None
            record.location = location if location else None
            record.sample_types_json = json.dumps(sample_data) if sample_data else None
            record.notes = notes if notes else None
            record.moto_fee = moto_fee
            record.far_away_fee = far_away_fee

            db.session.commit()

            cache_delete(f'dashboard_{session["user_id"]}_{cambodia_today().strftime("%Y-%m-%d")}')

            add_system_log(session['user_id'], 'Update sampling',
                           f'User updated record #{record.id}')
            flash('✅ Record updated successfully!', 'success')
            return redirect(url_for('dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error: {str(e)}', 'danger')

    return render_template('edit_record.html',
                           record=record,
                           call_time_value=call_time_value,
                           receive_time_value=receive_time_value,
                           existing_samples=existing_samples,
                           sample_types=SAMPLE_TYPES)


@app.route('/delete_record/<int:record_id>')
@login_required
def delete_record(record_id):
    record = CollectionRecord.query.get_or_404(record_id)
    if record.member_id != session['user_id'] and not session.get('is_admin'):
        flash('Unauthorized.', 'danger')
        return redirect(url_for('dashboard'))

    member = db.session.get(Member, session['user_id'])
    add_system_log(session['user_id'], 'Delete sampling', f'{member.username} deleted record #{record.id}')

    db.session.delete(record)
    db.session.commit()

    cache_delete(f'dashboard_{session["user_id"]}_{cambodia_today().strftime("%Y-%m-%d")}')

    flash('🗑️ Record deleted!', 'success')
    return redirect(url_for('dashboard'))


# ==================== OPTIMIZED REPORT ====================
@app.route('/report')
@login_required
def report():
    member = db.session.get(Member, session['user_id'])
    report_type = request.args.get('type', 'monthly')
    report_value = request.args.get('value', '')
    from_date = request.args.get('from_date', '').strip()
    to_date = request.args.get('to_date', '').strip()
    filter_collector = request.args.get('collector', '').strip()
    filter_facility = request.args.get('facility', '').strip()
    filter_sample_type = request.args.get('sample_type', '').strip()

    add_system_log(session['user_id'], 'Generate report', f'{member.username} generated {report_type} report')

    # Build base query
    if session.get('is_admin') and filter_collector:
        collector_member = Member.query.filter_by(username=filter_collector).first()
        if collector_member:
            query = CollectionRecord.query.filter(
                CollectionRecord.member_id == collector_member.id,
                EXCLUDE_FEE_ONLY
            )
        else:
            query = CollectionRecord.query.filter(EXCLUDE_FEE_ONLY)
    else:
        query = CollectionRecord.query.filter(
            CollectionRecord.member_id == session['user_id'],
            EXCLUDE_FEE_ONLY
        )

    today = cambodia_today()

    # Store filter conditions for aggregate query
    filter_conditions = []

    # Date filtering
    if from_date and to_date:
        try:
            from_dt = datetime.strptime(from_date, '%Y-%m-%d').date()
            to_dt = datetime.strptime(to_date, '%Y-%m-%d').date()
            if from_dt > to_dt:
                flash('From date must be earlier than To date.', 'warning')
                return redirect(url_for('report', type='monthly', value=today.strftime('%Y-%m')))
            query = query.filter(CollectionRecord.date >= from_dt, CollectionRecord.date <= to_dt)
            filter_conditions.append(CollectionRecord.date >= from_dt)
            filter_conditions.append(CollectionRecord.date <= to_dt)
            report_type = 'custom'
            report_value = f"{from_date} → {to_date}"
        except ValueError:
            flash('Invalid date format. Use YYYY-MM-DD.', 'danger')
            return redirect(url_for('report', type='monthly', value=today.strftime('%Y-%m')))
    else:
        if report_type == 'daily' and report_value:
            try:
                d = datetime.strptime(report_value, '%Y-%m-%d').date()
                query = query.filter(CollectionRecord.date == d)
                filter_conditions.append(CollectionRecord.date == d)
            except:
                report_value = today.strftime('%Y-%m-%d')
                query = query.filter(CollectionRecord.date == today)
                filter_conditions.append(CollectionRecord.date == today)
        elif report_type == 'monthly':
            if report_value:
                try:
                    d = datetime.strptime(report_value + '-01', '%Y-%m-%d').date()
                    query = query.filter(
                        db.extract('month', CollectionRecord.date) == d.month,
                        db.extract('year', CollectionRecord.date) == d.year
                    )
                    filter_conditions.append(db.extract('month', CollectionRecord.date) == d.month)
                    filter_conditions.append(db.extract('year', CollectionRecord.date) == d.year)
                except:
                    report_value = today.strftime('%Y-%m')
                    query = query.filter(
                        db.extract('month', CollectionRecord.date) == today.month,
                        db.extract('year', CollectionRecord.date) == today.year
                    )
                    filter_conditions.append(db.extract('month', CollectionRecord.date) == today.month)
                    filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)
            else:
                report_value = today.strftime('%Y-%m')
                query = query.filter(
                    db.extract('month', CollectionRecord.date) == today.month,
                    db.extract('year', CollectionRecord.date) == today.year
                )
                filter_conditions.append(db.extract('month', CollectionRecord.date) == today.month)
                filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)
        elif report_type == 'yearly':
            if report_value:
                try:
                    year = int(report_value)
                    query = query.filter(db.extract('year', CollectionRecord.date) == year)
                    filter_conditions.append(db.extract('year', CollectionRecord.date) == year)
                except:
                    report_value = str(today.year)
                    query = query.filter(db.extract('year', CollectionRecord.date) == today.year)
                    filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)
            else:
                report_value = str(today.year)
                query = query.filter(db.extract('year', CollectionRecord.date) == today.year)
                filter_conditions.append(db.extract('year', CollectionRecord.date) == today.year)

    if filter_facility:
        query = query.filter(CollectionRecord.location.ilike(f'%{filter_facility}%'))
        filter_conditions.append(CollectionRecord.location.ilike(f'%{filter_facility}%'))

    # Get records with limit for performance
    records = query.order_by(
        CollectionRecord.date.desc(),
        CollectionRecord.call_time.asc()
    ).limit(2000).all()

    # Filter by sample type in Python
    if filter_sample_type:
        filtered_records = []
        for r in records:
            if r.sample_types_json:
                try:
                    data = json.loads(r.sample_types_json)
                    if any(stype.lower() == filter_sample_type.lower() for stype in data.keys()):
                        filtered_records.append(r)
                except:
                    pass
        records = filtered_records

    # Use database aggregations with explicit filter conditions
    if not filter_sample_type:
        # Build aggregate query with the same filters
        agg_query = db.session.query(
            func.count(CollectionRecord.id).label('count'),
            func.sum(CollectionRecord.patient_count).label('patients'),
            func.sum(CollectionRecord.total_sampling).label('specimens'),
            func.sum(CollectionRecord.moto_fee).label('moto'),
            func.sum(CollectionRecord.far_away_fee).label('far')
        )

        # Apply base filters
        if session.get('is_admin') and filter_collector:
            collector_member = Member.query.filter_by(username=filter_collector).first()
            if collector_member:
                agg_query = agg_query.filter(CollectionRecord.member_id == collector_member.id)
        else:
            agg_query = agg_query.filter(CollectionRecord.member_id == session['user_id'])

        agg_query = agg_query.filter(EXCLUDE_FEE_ONLY)

        # Apply date and facility filters
        for condition in filter_conditions:
            agg_query = agg_query.filter(condition)

        aggregates = agg_query.first()

        total_records = aggregates.count or 0 if aggregates else 0
        total_patients = aggregates.patients or 0 if aggregates else 0
        total_specimens = aggregates.specimens or 0 if aggregates else 0
        total_moto_fee = aggregates.moto or 0 if aggregates else 0
        total_far_away_fee = aggregates.far or 0 if aggregates else 0
    else:
        # Fallback to Python calculations when sample type filter is applied
        total_records = len(records)
        total_patients = sum(r.patient_count for r in records)
        total_specimens = sum(r.total_sampling for r in records)
        total_moto_fee = sum(r.moto_fee for r in records)
        total_far_away_fee = sum(r.far_away_fee for r in records)

    total_fees = total_moto_fee + total_far_away_fee
    avg_specimens_per_patient = round(total_specimens / total_patients, 2) if total_patients > 0 else 0

    # Use set comprehension for faster uniqueness
    dates = {r.date for r in records}
    collection_days = len(dates)

    facilities = {r.location for r in records if r.location}
    doctors = {r.doctor for r in records if r.doctor}
    total_facilities = len(facilities)
    total_doctors = len(doctors)

    # Location summary using defaultdict
    location_summary = defaultdict(lambda: {'patients': 0, 'specimens': 0, 'count': 0})
    for r in records:
        loc = r.location or 'Unknown'
        location_summary[loc]['patients'] += r.patient_count
        location_summary[loc]['specimens'] += r.total_sampling
        location_summary[loc]['count'] += 1

    # Type summary
    type_summary = defaultdict(int)
    for r in records:
        if r.sample_types_json:
            try:
                data = json.loads(r.sample_types_json)
                for stype, count in data.items():
                    type_summary[stype] += count
            except:
                pass

    # Daily summary
    daily_summary = defaultdict(lambda: {'patients': 0, 'specimens': 0, 'count': 0})
    for r in records:
        d = r.date.strftime('%Y-%m-%d')
        daily_summary[d]['patients'] += r.patient_count
        daily_summary[d]['specimens'] += r.total_sampling
        daily_summary[d]['count'] += 1

    daily_list = []
    for d, vals in sorted(daily_summary.items()):
        avg = round(vals['specimens'] / vals['patients'], 2) if vals['patients'] > 0 else 0
        daily_list.append({
            'date': d,
            'patients': vals['patients'],
            'specimens': vals['specimens'],
            'avg': avg
        })

    # Collector performance (admin only)
    collector_performance = None
    if session.get('is_admin'):
        collector_data = defaultdict(lambda: {'full_name': 'Unknown', 'patients': 0, 'specimens': 0, 'days': set()})
        member_cache = {}
        for r in records:
            mid = r.member_id
            if mid not in member_cache:
                member_obj = db.session.get(Member, mid)
                member_cache[mid] = member_obj.full_name if member_obj else 'Unknown'
            collector_data[mid]['full_name'] = member_cache[mid]
            collector_data[mid]['patients'] += r.patient_count
            collector_data[mid]['specimens'] += r.total_sampling
            collector_data[mid]['days'].add(r.date)

        collector_performance = [
            {
                'full_name': data['full_name'],
                'patients': data['patients'],
                'specimens': data['specimens'],
                'days': len(data['days'])
            }
            for mid, data in collector_data.items()
        ]
        collector_performance.sort(key=lambda x: x['specimens'], reverse=True)

    # Duration calculation
    total_duration_minutes = 0
    duration_count = 0
    for r in records:
        if r.duration:
            minutes = 0
            parts = r.duration.split()
            for part in parts:
                if 'h' in part:
                    minutes += int(part.replace('h', '')) * 60
                elif 'm' in part:
                    minutes += int(part.replace('m', ''))
            if minutes > 0:
                total_duration_minutes += minutes
                duration_count += 1
    avg_duration_minutes = round(total_duration_minutes / duration_count, 0) if duration_count > 0 else 0
    avg_duration_display = f"{int(avg_duration_minutes)} minutes" if avg_duration_minutes > 0 else "N/A"

    notes_records = [r for r in records if r.notes]

    all_collectors = []
    if session.get('is_admin'):
        all_collectors = Member.query.filter_by(is_active=True).order_by(Member.full_name).all()
    all_facilities = sorted({r.location for r in records if r.location})
    all_sample_types = sorted(type_summary.keys())

    return render_template('report.html',
                           member=member,
                           records=records[:500],
                           total_records=total_records,
                           total_patients=total_patients,
                           total_specimens=total_specimens,
                           avg_specimens_per_patient=avg_specimens_per_patient,
                           collection_days=collection_days,
                           total_facilities=total_facilities,
                           total_doctors=total_doctors,
                           location_summary=location_summary,
                           type_summary=type_summary,
                           daily_summary=daily_list,
                           collector_performance=collector_performance,
                           avg_duration_display=avg_duration_display,
                           notes_records=notes_records[:50],
                           total_moto_fee=total_moto_fee,
                           total_far_away_fee=total_far_away_fee,
                           total_fees=total_fees,
                           report_type=report_type,
                           report_value=report_value,
                           today=today,
                           now=cambodia_now(),
                           from_date=from_date,
                           to_date=to_date,
                           filter_collector=filter_collector,
                           filter_facility=filter_facility,
                           filter_sample_type=filter_sample_type,
                           all_collectors=all_collectors,
                           all_facilities=all_facilities,
                           all_sample_types=all_sample_types,
                           get_sample_types_display=get_sample_types_display)


@app.route('/export_csv')
@login_required
def export_csv():
    from_date = request.args.get('from_date', '').strip()
    to_date = request.args.get('to_date', '').strip()
    collector = request.args.get('collector', '').strip()

    if session.get('is_admin') and collector:
        member = Member.query.filter_by(username=collector).first()
        if member:
            query = CollectionRecord.query.filter_by(member_id=member.id).filter(EXCLUDE_FEE_ONLY)
        else:
            query = CollectionRecord.query.filter(EXCLUDE_FEE_ONLY)
    else:
        query = CollectionRecord.query.filter_by(member_id=session['user_id']).filter(EXCLUDE_FEE_ONLY)

    if from_date and to_date:
        try:
            from_dt = datetime.strptime(from_date, '%Y-%m-%d').date()
            to_dt = datetime.strptime(to_date, '%Y-%m-%d').date()
            query = query.filter(CollectionRecord.date >= from_dt, CollectionRecord.date <= to_dt)
        except ValueError:
            pass

    records = query.order_by(CollectionRecord.date.desc()).limit(10000).all()

    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['Date', 'Location', 'Doctor', 'Call Time', 'Receive Time',
                     'Patients', 'Specimens', 'Moto Fee', 'Far Fee', 'Total Fee', 'Notes'])

    for r in records:
        writer.writerow([
            r.date.strftime('%Y-%m-%d'),
            r.location or '',
            r.doctor or '',
            r.call_time or '',
            r.receive_time or '',
            r.patient_count,
            r.total_sampling,
            r.moto_fee or 0,
            r.far_away_fee or 0,
            (r.moto_fee or 0) + (r.far_away_fee or 0),
            r.notes or ''
        ])

    output = si.getvalue()
    si.close()

    response = make_response(output)
    response.headers['Content-Disposition'] = 'attachment; filename=summary_report.csv'
    response.headers['Content-type'] = 'text/csv'
    return response


@app.route('/print_report')
@login_required
def print_report():
    member = db.session.get(Member, session['user_id'])
    report_type = request.args.get('type', 'monthly')
    report_value = request.args.get('value', '')

    role = 'Admin' if session.get('is_admin') else 'Collector'
    add_system_log(session['user_id'], 'Print report', f'{role} printed {report_type} report')

    query = CollectionRecord.query.filter_by(member_id=session['user_id']).filter(EXCLUDE_FEE_ONLY)
    today = cambodia_today()
    title = "Monthly Report"

    if report_type == 'daily' and report_value:
        try:
            d = datetime.strptime(report_value, '%Y-%m-%d').date()
            query = query.filter(CollectionRecord.date == d)
            title = f"Daily Report - {report_value}"
        except:
            pass
    elif report_type == 'monthly' and report_value:
        try:
            d = datetime.strptime(report_value + '-01', '%Y-%m-%d').date()
            query = query.filter(db.extract('month', CollectionRecord.date) == d.month,
                                 db.extract('year', CollectionRecord.date) == d.year)
            title = f"Monthly Report - {report_value}"
        except:
            pass
    elif report_type == 'yearly' and report_value:
        try:
            year = int(report_value)
            query = query.filter(db.extract('year', CollectionRecord.date) == year)
            title = f"Yearly Report - {report_value}"
        except:
            pass

    records = query.order_by(CollectionRecord.date.desc(), CollectionRecord.call_time.asc()).limit(1000).all()

    total_records = len(records)
    total_sampling = sum(r.total_sampling for r in records)
    avg = round(total_sampling / total_records, 2) if total_records > 0 else 0
    highest = max((r.total_sampling for r in records), default=0)

    total_moto_fee = sum(r.moto_fee for r in records)
    total_far_away_fee = sum(r.far_away_fee for r in records)
    total_fees = total_moto_fee + total_far_away_fee

    location_summary = {}
    for r in records:
        loc = r.location or 'Unknown'
        if loc not in location_summary:
            location_summary[loc] = {'total': 0, 'count': 0}
        location_summary[loc]['total'] += r.total_sampling
        location_summary[loc]['count'] += 1

    return render_template('print_report.html',
                           member=member,
                           records=records,
                           total_records=total_records,
                           total_sampling=total_sampling,
                           average_sampling=avg,
                           highest=highest,
                           location_summary=location_summary,
                           title=title,
                           today=today,
                           now=cambodia_now(),
                           total_moto_fee=total_moto_fee,
                           total_far_away_fee=total_far_away_fee,
                           total_fees=total_fees,
                           get_sample_types_display=get_sample_types_display)


# ==================== FEE INPUT ROUTES ====================
@app.route('/fee_input', methods=['GET', 'POST'])
@login_required
def fee_input():
    if request.method == 'POST':
        collection_date = request.form.get('collection_date', '').strip()
        location = request.form.get('location', '').strip()
        moto_fee = request.form.get('moto_fee', type=int) or 0
        far_away_fee = request.form.get('far_away_fee', type=int) or 0
        notes = request.form.get('notes', '').strip()

        errors = []
        if not collection_date:
            errors.append("Collection date is required.")
        if not location:
            errors.append("Location is required.")

        if errors:
            for err in errors:
                flash(err, 'danger')
            return redirect(url_for('fee_input'))

        try:
            record_date = datetime.strptime(collection_date, '%Y-%m-%d').date()
            new_record = CollectionRecord(
                member_id=session['user_id'],
                date=record_date,
                location=location,
                moto_fee=moto_fee,
                far_away_fee=far_away_fee,
                notes=notes if notes else None,
                patient_count=0,
                total_sampling=0,
                call_time=None,
                receive_time=None,
                duration=None,
                doctor=None,
                sample_types_json=None
            )
            db.session.add(new_record)
            db.session.commit()
            add_system_log(session['user_id'], 'Add fee record',
                           f'Added record: location={location}, moto={moto_fee}, far={far_away_fee}')
            flash('✅ Record saved successfully!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'❌ Database error: {str(e)}', 'danger')
        return redirect(url_for('fee_input'))

    if session.get('is_admin'):
        query = CollectionRecord.query
    else:
        query = CollectionRecord.query.filter_by(member_id=session['user_id'])

    query = query.filter(
        CollectionRecord.patient_count == 0,
        CollectionRecord.total_sampling == 0
    )

    records = query.order_by(CollectionRecord.date.desc(), CollectionRecord.id.desc()).limit(50).all()

    grouped = {}
    for r in records:
        key = f"{r.date.strftime('%Y-%m-%d')}|{r.location or 'Unknown'}"
        if key not in grouped:
            grouped[key] = {
                'date': r.date,
                'location': r.location or 'Unknown',
                'count': 0,
                'moto_fee': 0,
                'far_away_fee': 0,
                'records': []
            }
        grouped[key]['count'] += 1
        grouped[key]['moto_fee'] += r.moto_fee or 0
        grouped[key]['far_away_fee'] += r.far_away_fee or 0
        grouped[key]['records'].append(r)

    grouped_list = sorted(grouped.values(), key=lambda x: x['date'], reverse=True)
    total_moto = sum(g['moto_fee'] for g in grouped_list)
    total_far = sum(g['far_away_fee'] for g in grouped_list)

    return render_template('fee_input.html',
                           today=cambodia_today(),
                           grouped=grouped_list,
                           total_moto=total_moto,
                           total_far=total_far)


@app.route('/update_group/<path:group_key>', methods=['POST'])
@login_required
def update_group(group_key):
    data = request.get_json()
    moto = data.get('moto', 0)
    far = data.get('far', 0)
    new_location = data.get('location', '').strip()
    parts = group_key.split('|')
    if len(parts) != 2:
        return jsonify({'success': False, 'message': 'Invalid group key'}), 400
    date_str, location = parts
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'success': False, 'message': 'Invalid date'}), 400

    query = CollectionRecord.query.filter(
        CollectionRecord.date == date_obj,
        CollectionRecord.location == location,
        CollectionRecord.patient_count == 0,
        CollectionRecord.total_sampling == 0
    )
    if not session.get('is_admin'):
        query = query.filter_by(member_id=session['user_id'])

    records = query.all()
    if not records:
        return jsonify({'success': False, 'message': 'No records found'}), 404

    for r in records:
        r.moto_fee = moto
        r.far_away_fee = far
        if new_location:
            r.location = new_location
    db.session.commit()
    return jsonify({
        'success': True,
        'count': len(records),
        'moto': moto,
        'far': far,
        'location': new_location or location,
        'new_key': f"{date_str}|{new_location or location}"
    })


@app.route('/delete_group/<path:group_key>')
@login_required
def delete_group(group_key):
    parts = group_key.split('|')
    if len(parts) != 2:
        flash('Invalid group key.', 'danger')
        return redirect(url_for('fee_input'))
    date_str, location = parts
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Invalid date format.', 'danger')
        return redirect(url_for('fee_input'))

    query = CollectionRecord.query.filter(
        CollectionRecord.date == date_obj,
        CollectionRecord.location == location,
        CollectionRecord.patient_count == 0,
        CollectionRecord.total_sampling == 0
    )
    if not session.get('is_admin'):
        query = query.filter_by(member_id=session['user_id'])

    records = query.all()
    if not records:
        flash('No records found in this group.', 'warning')
        return redirect(url_for('fee_input'))

    count = len(records)
    for r in records:
        db.session.delete(r)
    db.session.commit()
    add_system_log(session['user_id'], 'Delete fee group',
                   f'Deleted {count} fee records for {date_str} at {location}')
    flash(f'🗑️ Deleted {count} records from {date_str} at {location}.', 'success')
    return redirect(url_for('fee_input'))


@app.route('/get_group_records/<path:group_key>')
@login_required
def get_group_records(group_key):
    parts = group_key.split('|')
    if len(parts) != 2:
        return jsonify({'success': False, 'message': 'Invalid group key'}), 400
    date_str, location = parts
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'success': False, 'message': 'Invalid date'}), 400

    query = CollectionRecord.query.filter(
        CollectionRecord.date == date_obj,
        CollectionRecord.location == location,
        CollectionRecord.patient_count == 0,
        CollectionRecord.total_sampling == 0
    )
    if not session.get('is_admin'):
        query = query.filter_by(member_id=session['user_id'])

    records = query.order_by(CollectionRecord.id.asc()).all()
    data = []
    for r in records:
        data.append({
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d'),
            'location': r.location,
            'moto_fee': r.moto_fee or 0,
            'far_away_fee': r.far_away_fee or 0,
            'notes': r.notes or '-'
        })
    return jsonify({'success': True, 'records': data})


@app.route('/update_fee', methods=['POST'])
@login_required
def update_fee():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided'}), 400

    record_id = data.get('id')
    moto = data.get('moto_fee')
    far = data.get('far_away_fee')

    if not record_id:
        return jsonify({'success': False, 'message': 'Record ID missing'}), 400

    record = CollectionRecord.query.get(record_id)
    if not record:
        return jsonify({'success': False, 'message': 'Record not found'}), 404

    if record.member_id != session['user_id'] and not session.get('is_admin'):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    try:
        record.moto_fee = float(moto) if moto is not None else 0
        record.far_away_fee = float(far) if far is not None else 0
        db.session.commit()
        add_system_log(session['user_id'], 'Update fee', f'Updated fees for record #{record_id}')
        return jsonify({'success': True, 'message': 'Fee updated successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/update_fees', methods=['POST'])
@login_required
def update_fees():
    try:
        for key, value in request.form.items():
            if key.startswith('moto_'):
                rec_id = key.split('_')[1]
                record = CollectionRecord.query.get(rec_id)
                if record:
                    if record.member_id != session['user_id'] and not session.get('is_admin'):
                        continue
                    record.moto_fee = float(value) if value else 0
            elif key.startswith('far_'):
                rec_id = key.split('_')[1]
                record = CollectionRecord.query.get(rec_id)
                if record:
                    if record.member_id != session['user_id'] and not session.get('is_admin'):
                        continue
                    record.far_away_fee = float(value) if value else 0

        db.session.commit()
        add_system_log(session['user_id'], 'Bulk fee update', 'Updated multiple fees')
        return jsonify({'success': True, 'message': 'All fees saved successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== PROFILE ROUTE ====================
@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    member = db.session.get(Member, session['user_id'])
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'update_profile':
            member.full_name = request.form.get('full_name', member.full_name)
            member.email = request.form.get('email', '').strip() or None
            member.phone = request.form.get('phone', '').strip() or None

            image_data = request.form.get('profile_image_data', '')

            if image_data and image_data.startswith('data:image'):
                try:
                    header, encoded = image_data.split(',', 1)
                    image_bytes = base64.b64decode(encoded)

                    if len(image_bytes) > 400 * 1024:
                        flash('❌ Image too large. Please use a smaller image.', 'danger')
                        return redirect(url_for('profile'))

                    new_filename = None
                    old_filename = member.profile_picture

                    if USE_CLOUDINARY:
                        from io import BytesIO
                        upload_result = cloudinary.uploader.upload(
                            BytesIO(image_bytes),
                            folder=f"cammedlab/profiles/{member.id}",
                            transformation=[
                                {'width': 150, 'height': 150, 'crop': 'thumb', 'gravity': 'face'},
                                {'quality': 'auto:low'},
                                {'fetch_format': 'auto'}
                            ],
                            public_id=f"member_{member.id}_{secrets.token_hex(8)}"
                        )
                        new_filename = upload_result.get('secure_url')
                        print(f"✅ Profile picture uploaded to Cloudinary: {new_filename}")
                    else:
                        ext = 'png' if 'png' in header else 'jpg'
                        new_filename = f"member_{member.id}_{int(time_module.time())}_{secrets.token_hex(8)}.{ext}"
                        filepath = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)
                        img = Image.open(io.BytesIO(image_bytes))
                        if img.mode in ('RGBA', 'LA', 'P'):
                            img = img.convert('RGB')
                        max_size = app.config.get('MAX_IMAGE_SIZE', 150)
                        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                        if ext == 'png':
                            img.save(filepath, 'PNG', optimize=True)
                        else:
                            img.save(filepath, 'JPEG', quality=60, optimize=True)
                        print(f"✅ Profile picture saved locally: {new_filename}")

                    # Delete old picture if not default
                    if old_filename and old_filename != 'default.png':
                        if not USE_CLOUDINARY and not old_filename.startswith('http'):
                            old = os.path.join(app.config['UPLOAD_FOLDER'], old_filename)
                            if os.path.exists(old):
                                try:
                                    os.remove(old)
                                    print(f"✅ Deleted old profile picture: {old_filename}")
                                except Exception as e:
                                    print(f"⚠️ Could not delete old profile picture: {e}")

                    member.profile_picture = new_filename
                    session['profile_picture'] = new_filename
                    session['profile_version'] = session.get('profile_version', 1) + 1

                    db.session.commit()

                    # Clear cache
                    cache_delete(f'profile_{member.id}')

                    flash('✅ Profile picture updated!', 'success')

                except Exception as e:
                    print(f"❌ Error processing image: {e}")
                    flash('❌ Could not process image. Please try again.', 'danger')
                    return redirect(url_for('profile'))

            elif 'profile_picture' in request.files:
                file = request.files['profile_picture']
                if file and file.filename:
                    file.seek(0, 2)
                    file_size = file.tell()
                    file.seek(0)

                    if file_size > app.config['MAX_CONTENT_LENGTH']:
                        flash('❌ Profile picture is too large. Please use an image under 500KB.', 'danger')
                        return redirect(url_for('profile'))

                    old_filename = member.profile_picture

                    filename = save_profile_picture(file, member.id)
                    if filename != 'default.png':
                        # Delete old picture if not default
                        if old_filename and old_filename != 'default.png':
                            if not USE_CLOUDINARY and not old_filename.startswith('http'):
                                old = os.path.join(app.config['UPLOAD_FOLDER'], old_filename)
                                if os.path.exists(old):
                                    try:
                                        os.remove(old)
                                    except:
                                        pass

                        member.profile_picture = filename
                        session['profile_picture'] = filename
                        session['profile_version'] = session.get('profile_version', 1) + 1
                        db.session.commit()

                        cache_delete(f'profile_{member.id}')

                        flash('✅ Profile picture updated!', 'success')
                        print(f"✅ Image saved via file upload: {filename}")
                    else:
                        flash('❌ Could not save profile picture. Please try again.', 'danger')
                        return redirect(url_for('profile'))

            now = cambodia_now()
            member.is_online = True
            member.last_login = now

            db.session.commit()

            # Update session
            session['full_name'] = member.full_name
            if member.profile_picture:
                session['profile_picture'] = member.profile_picture

            broadcast_status_update(member.id, True, {
                'full_name': member.full_name,
                'username': member.username,
                'profile_picture': member.profile_picture
            })

            flash('✅ Profile updated!', 'success')
            return redirect(url_for('profile'))

        elif action == 'change_password':
            current = request.form.get('current_password')
            new = request.form.get('new_password')
            confirm = request.form.get('confirm_password')
            if not member.check_password(current):
                flash('❌ Current password incorrect.', 'danger')
            elif len(new) < 6:
                flash('❌ Password must be 6+ characters.', 'danger')
            elif new != confirm:
                flash('❌ Passwords do not match.', 'danger')
            else:
                member.set_password(new)
                now = cambodia_now()
                member.is_online = True
                member.last_login = now
                db.session.commit()
                add_system_log(session['user_id'], 'Change password', f'{member.username} changed password')
                flash('✅ Password changed!', 'success')
            return redirect(url_for('profile'))

    total_records = CollectionRecord.query.filter_by(member_id=member.id).filter(EXCLUDE_FEE_ONLY).count()
    total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
        CollectionRecord.member_id == member.id,
        EXCLUDE_FEE_ONLY
    ).scalar() or 0
    member_since = member.created_at.strftime('%B %d, %Y') if member.created_at else 'N/A'
    return render_template('profile.html',
                           member=member,
                           total_records=total_records,
                           total_sampling=total_sampling,
                           member_since=member_since,
                           USE_CLOUDINARY=USE_CLOUDINARY)

@app.route('/uploads/profiles/<path:filename>')
def uploaded_file(filename):
    """Serve profile pictures - handles both local files and Cloudinary URLs"""
    # If it's a Cloudinary URL, redirect to it
    if filename and (filename.startswith('http://') or filename.startswith('https://')):
        return redirect(filename)

    upload_folder = app.config['UPLOAD_FOLDER']
    os.makedirs(upload_folder, exist_ok=True)

    file_path = os.path.join(upload_folder, filename)

    # If file exists, serve it
    if os.path.exists(file_path):
        return send_from_directory(upload_folder, filename)

    # Try to find a member profile
    if filename.startswith('member_'):
        parts = filename.split('_')
        if len(parts) >= 2:
            member_id = parts[1]
            pattern = os.path.join(upload_folder, f"member_{member_id}_*")
            try:
                existing_files = glob.glob(pattern)
                if existing_files:
                    latest = max(existing_files, key=os.path.getctime)
                    return send_from_directory(upload_folder, os.path.basename(latest))
            except Exception as e:
                print(f"⚠️ Error: {e}")

    # Check for default.png
    default_path = os.path.join(upload_folder, 'default.png')
    if os.path.exists(default_path):
        return send_from_directory(upload_folder, 'default.png')

    # Create default.png if it doesn't exist
    try:
        from io import BytesIO
        img = Image.new('RGB', (300, 300), color='#2b5d8d')
        img.save(default_path, 'PNG')
        return send_from_directory(upload_folder, 'default.png')
    except Exception as e:
        print(f"⚠️ Could not create default: {e}")
        # Fallback: serve from memory
        from io import BytesIO
        img = Image.new('RGB', (300, 300), color='#2b5d8d')
        img_io = BytesIO()
        img.save(img_io, 'PNG')
        img_io.seek(0)
        return send_file(img_io, mimetype='image/png')

# ==================== OPTIMIZED MEMBERS LIST ====================
@app.route('/members')
@login_required
def members_list():
    members = Member.query.order_by(Member.full_name.asc()).all()

    member_ids = [m.id for m in members]

    # Bulk query for all statistics
    stats_query = db.session.query(
        CollectionRecord.member_id,
        func.count(CollectionRecord.id).label('record_count'),
        func.sum(CollectionRecord.total_sampling).label('total_sampling'),
        func.sum(CollectionRecord.patient_count).label('total_patients'),
        func.max(CollectionRecord.date).label('last_record_date')
    ).filter(
        CollectionRecord.member_id.in_(member_ids),
        EXCLUDE_FEE_ONLY
    ).group_by(CollectionRecord.member_id).all()

    stats_map = {s.member_id: s for s in stats_query}

    # Bulk query for last record details
    latest_records = {}
    if member_ids:
        latest_subquery = db.session.query(
            CollectionRecord.member_id,
            func.max(CollectionRecord.created_at).label('max_created')
        ).filter(
            CollectionRecord.member_id.in_(member_ids),
            EXCLUDE_FEE_ONLY
        ).group_by(CollectionRecord.member_id).subquery()

        latest = db.session.query(CollectionRecord).join(
            latest_subquery,
            db.and_(
                CollectionRecord.member_id == latest_subquery.c.member_id,
                CollectionRecord.created_at == latest_subquery.c.max_created
            )
        ).all()

        for r in latest:
            latest_records[r.member_id] = r

    member_data = []
    grand_total_records = 0
    grand_total_sampling = 0
    grand_total_patients = 0

    for m in members:
        stats = stats_map.get(m.id)
        record_count = stats.record_count if stats else 0
        total_sampling = stats.total_sampling if stats else 0
        total_patients = stats.total_patients if stats else 0

        last_record = latest_records.get(m.id)
        if last_record:
            if last_record.created_at:
                last_record_date = last_record.created_at.strftime('%Y-%m-%d %H:%M')
            else:
                last_record_date = last_record.date.strftime('%Y-%m-%d')
        else:
            last_record_date = 'Never'

        if m.last_login:
            last_seen = m.last_login.strftime('%Y-%m-%d %H:%M')
        else:
            last_seen = 'Never'

        member_data.append({
            'id': m.id,
            'username': m.username,
            'full_name': m.full_name,
            'email': m.email or '-',
            'phone': m.phone or '',
            'role': 'Admin' if m.is_admin else 'Member',
            'status': 'Active' if m.is_active else 'Inactive',
            'record_count': record_count,
            'total_sampling': total_sampling,
            'total_patients': total_patients,
            'last_record': last_record_date,
            'profile_picture': m.profile_picture,
            'created_at': m.created_at.strftime('%Y-%m-%d %H:%M') if m.created_at else 'N/A',
            'is_online': m.is_online,
            'last_seen': last_seen if not m.is_online else 'Online now'
        })

        grand_total_records += record_count
        grand_total_sampling += total_sampling
        grand_total_patients += total_patients

    return render_template('members_list.html',
                           member_data=member_data,
                           grand_total_records=grand_total_records,
                           grand_total_sampling=grand_total_sampling,
                           grand_total_patients=grand_total_patients,
                           total_members=len(members),
                           today=cambodia_today())


# ==================== MEMBER DETAILS ====================
@app.route('/member/<int:member_id>/details')
@login_required
def member_details(member_id):
    member = Member.query.get_or_404(member_id)

    base_q = CollectionRecord.query.filter(
        CollectionRecord.member_id == member.id,
        EXCLUDE_FEE_ONLY
    )

    record_count = base_q.count()
    total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
        CollectionRecord.member_id == member.id,
        EXCLUDE_FEE_ONLY
    ).scalar() or 0
    total_patients = db.session.query(func.sum(CollectionRecord.patient_count)).filter(
        CollectionRecord.member_id == member.id,
        EXCLUDE_FEE_ONLY
    ).scalar() or 0

    records = base_q.order_by(CollectionRecord.date.desc()).limit(500).all()

    location_summary = {}
    for r in records:
        loc = r.location or 'Unknown'
        if loc not in location_summary:
            location_summary[loc] = {'total': 0, 'count': 0}
        location_summary[loc]['total'] += r.total_sampling
        location_summary[loc]['count'] += 1

    type_summary = {}
    for r in records:
        if r.sample_types_json:
            try:
                data = json.loads(r.sample_types_json)
                for stype, count in data.items():
                    if stype not in type_summary:
                        type_summary[stype] = 0
                    type_summary[stype] += count
            except:
                pass

    total_moto_fee = sum(r.moto_fee for r in records)
    total_far_away_fee = sum(r.far_away_fee for r in records)
    total_fees = total_moto_fee + total_far_away_fee

    if records:
        last_record = records[0].date.strftime('%Y-%m-%d')
        first_record = records[-1].date.strftime('%Y-%m-%d')
    else:
        last_record = 'Never'
        first_record = 'Never'

    return render_template('member_details.html',
                           member=member,
                           records=records,
                           record_count=record_count,
                           total_sampling=total_sampling,
                           total_patients=total_patients,
                           location_summary=location_summary,
                           type_summary=type_summary,
                           last_record=last_record,
                           first_record=first_record,
                           total_moto_fee=total_moto_fee,
                           total_far_away_fee=total_far_away_fee,
                           total_fees=total_fees,
                           today=cambodia_today(),
                           get_sample_types_display=get_sample_types_display)


# ==================== ADMIN PANEL ====================
@app.route('/admin')
@admin_required
def admin_panel():
    members = Member.query.order_by(Member.created_at.desc()).all()
    total_records = CollectionRecord.query.filter(EXCLUDE_FEE_ONLY).count()
    total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
        EXCLUDE_FEE_ONLY).scalar() or 0
    total_samples_all = db.session.query(func.sum(CollectionRecord.patient_count)).filter(
        EXCLUDE_FEE_ONLY).scalar() or 0
    recent_logs = SystemLog.query.order_by(SystemLog.timestamp.desc()).limit(50).all()

    member_stats = {}
    for m in members:
        m_total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        m_total_samples = db.session.query(func.sum(CollectionRecord.patient_count)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        member_stats[m.id] = {
            'record_count': CollectionRecord.query.filter(
                CollectionRecord.member_id == m.id,
                EXCLUDE_FEE_ONLY
            ).count(),
            'total_sampling': m_total_sampling,
            'total_samples': m_total_samples
        }

    return render_template('admin.html',
                           members=members,
                           total_records=total_records,
                           total_sampling=total_sampling,
                           total_samples_all=total_samples_all,
                           recent_logs=recent_logs,
                           member_stats=member_stats)


# ==================== PASSWORD RESET REQUESTS (ADMIN) ====================
@app.route('/admin/reset_requests')
@admin_required
def admin_reset_requests():
    requests = PasswordResetRequest.query.filter_by(status='Pending').order_by(
        PasswordResetRequest.request_date.desc()
    ).all()
    pending_count = PasswordResetRequest.query.filter_by(status='Pending').count()
    return render_template('admin_reset_requests.html',
                           requests=requests,
                           pending_count=pending_count,
                           now=cambodia_now())


@app.route('/admin/reset_request/<int:request_id>/approve', methods=['POST'])
@admin_required
def admin_reset_request_approve(request_id):
    try:
        reset_request = PasswordResetRequest.query.get_or_404(request_id)
        if reset_request.status != 'Pending':
            return jsonify({'success': False, 'message': 'Request already processed'}), 400

        data = request.get_json()
        new_password = data.get('new_password', 'pass123') if data else 'pass123'
        if len(new_password) < 4:
            return jsonify({'success': False, 'message': 'Password must be at least 4 characters'}), 400

        reset_request.status = 'Approved'
        member = Member.query.filter_by(username=reset_request.username).first()
        if member:
            member.set_password(new_password)
            reset_request.admin_note = f'new_password:{new_password}'
            db.session.commit()
            admin = db.session.get(Member, session['user_id'])
            add_system_log(session['user_id'], 'Approve reset',
                           f'Admin {admin.username} approved password reset for {reset_request.username}')
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Password reset approved',
            'username': reset_request.username
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/admin/reset_request/<int:request_id>/reject', methods=['POST'])
@admin_required
def admin_reset_request_reject(request_id):
    try:
        reset_request = PasswordResetRequest.query.get_or_404(request_id)
        if reset_request.status != 'Pending':
            return jsonify({'success': False, 'message': 'Request already processed'}), 400

        reset_request.status = 'Rejected'
        db.session.commit()
        admin = db.session.get(Member, session['user_id'])
        add_system_log(session['user_id'], 'Reject reset',
                       f'Admin {admin.username} rejected password reset for {reset_request.username}')
        return jsonify({'success': True, 'message': 'Password reset rejected', 'username': reset_request.username})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/admin/reset_request/<int:request_id>/<action>')
@admin_required
def admin_reset_request_action(request_id, action):
    reset_request = PasswordResetRequest.query.get_or_404(request_id)
    if action == 'approve':
        reset_request.status = 'Approved'
        member = Member.query.filter_by(username=reset_request.username).first()
        if member:
            new_password = 'pass123'
            member.set_password(new_password)
            reset_request.admin_note = f'new_password:{new_password}'
            db.session.commit()
            admin = db.session.get(Member, session['user_id'])
            add_system_log(session['user_id'], 'Approve reset',
                           f'Admin {admin.username} approved password reset for {reset_request.username}')
        flash(f'✅ Password reset request for {reset_request.username} approved!', 'success')
    elif action == 'reject':
        reset_request.status = 'Rejected'
        db.session.commit()
        admin = db.session.get(Member, session['user_id'])
        add_system_log(session['user_id'], 'Reject reset',
                       f'Admin {admin.username} rejected password reset for {reset_request.username}')
        flash(f'❌ Password reset request for {reset_request.username} rejected.', 'warning')
    db.session.commit()
    return redirect(url_for('admin_reset_requests'))


# ==================== ADMIN MEMBER MANAGEMENT ====================
@app.route('/admin/add_member', methods=['POST'])
@admin_required
def admin_add_member():
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    full_name = request.form.get('full_name', '').strip()
    if not username or not password or not full_name:
        flash('All fields required.', 'danger')
        return redirect(url_for('admin_panel'))
    if Member.query.filter_by(username=username).first():
        flash('Username exists.', 'danger')
        return redirect(url_for('admin_panel'))
    member = Member(username=username, full_name=full_name, is_admin=request.form.get('is_admin') == 'on')
    member.set_password(password)
    db.session.add(member)
    db.session.commit()

    admin = db.session.get(Member, session['user_id'])
    add_system_log(session['user_id'], 'Create account', f'Admin created collector {username}')

    flash(f'✅ Member {full_name} added!', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/edit_member/<int:member_id>', methods=['POST'])
@admin_required
def admin_edit_member(member_id):
    member = Member.query.get_or_404(member_id)

    new_username = request.form.get('username', '').strip()
    if new_username and new_username != member.username:
        existing = Member.query.filter_by(username=new_username).first()
        if existing and existing.id != member.id:
            flash('Username already taken.', 'danger')
            return redirect(url_for('admin_panel'))
        member.username = new_username

    member.full_name = request.form.get('full_name', member.full_name)
    member.email = request.form.get('email', '').strip() or None
    member.phone = request.form.get('phone', '').strip() or None
    member.is_admin = request.form.get('is_admin') == 'on'
    member.is_active = request.form.get('is_active') == 'on'

    new_password = request.form.get('new_password')
    if new_password:
        member.set_password(new_password)

    db.session.commit()

    admin = db.session.get(Member, session['user_id'])
    add_system_log(session['user_id'], 'Update account', f'Admin updated {member.username} profile')

    broadcast_status_update(member.id, member.is_online, {
        'full_name': member.full_name,
        'username': member.username,
        'profile_picture': member.profile_picture
    })

    flash(f'✅ Member {member.full_name} updated successfully!', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/delete_member/<int:member_id>')
@admin_required
def admin_delete_member(member_id):
    if member_id == session['user_id']:
        flash('Cannot delete yourself.', 'danger')
        return redirect(url_for('admin_panel'))
    member = Member.query.get_or_404(member_id)

    broadcast_status_update(member.id, False, {
        'full_name': member.full_name,
        'username': member.username
    })

    db.session.delete(member)
    db.session.commit()

    admin = db.session.get(Member, session['user_id'])
    add_system_log(session['user_id'], 'Delete account', f'Admin deleted collector {member.username}')

    flash('🗑️ Member deleted!', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/toggle_member/<int:member_id>')
@admin_required
def admin_toggle_member(member_id):
    if member_id == session['user_id']:
        flash('Cannot toggle yourself.', 'danger')
        return redirect(url_for('admin_panel'))
    member = Member.query.get_or_404(member_id)
    member.is_active = not member.is_active
    if not member.is_active:
        member.is_online = False
        broadcast_status_update(member.id, False, {
            'full_name': member.full_name,
            'username': member.username
        })
    db.session.commit()
    flash(f'✅ Member {"activated" if member.is_active else "deactivated"}!', 'success')
    return redirect(url_for('admin_panel'))


@app.route('/admin/reset_password/<int:member_id>', methods=['POST'])
@admin_required
def admin_reset_password(member_id):
    member = Member.query.get_or_404(member_id)
    new_password = request.form.get('new_password', '').strip()
    if not new_password or len(new_password) < 6:
        flash('Password must be at least 6 characters.', 'danger')
        return redirect(url_for('admin_panel'))
    member.set_password(new_password)
    db.session.commit()
    add_system_log(session['user_id'], 'Reset password', f'Admin reset password for {member.username}')
    flash(f'✅ Password for {member.full_name} has been updated!', 'success')
    return redirect(url_for('admin_panel'))


# ==================== ADMIN REPORTS ====================
@app.route('/admin/report')
@admin_required
def admin_report():
    members = Member.query.order_by(Member.full_name.asc()).all()
    member_data = []
    grand_total_records = 0
    grand_total_sampling = 0
    grand_total_patients = 0
    grand_total_moto_fee = 0
    grand_total_far_away_fee = 0

    for m in members:
        record_count = CollectionRecord.query.filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).count()
        total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_patients = db.session.query(func.sum(CollectionRecord.patient_count)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_moto = db.session.query(func.sum(CollectionRecord.moto_fee)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_far = db.session.query(func.sum(CollectionRecord.far_away_fee)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        last_record = CollectionRecord.query.filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).order_by(CollectionRecord.date.desc()).first()
        last_record_date = last_record.date.strftime('%Y-%m-%d') if last_record else 'Never'

        member_data.append({
            'id': m.id,
            'username': m.username,
            'full_name': m.full_name,
            'email': m.email or '-',
            'phone': m.phone or '-',
            'role': 'Admin' if m.is_admin else 'Member',
            'status': 'Active' if m.is_active else 'Inactive',
            'record_count': record_count,
            'total_sampling': total_sampling,
            'total_patients': total_patients,
            'total_moto_fee': total_moto,
            'total_far_away_fee': total_far,
            'last_record': last_record_date,
            'profile_picture': m.profile_picture,
            'created_at': m.created_at.strftime('%Y-%m-%d') if m.created_at else 'N/A',
            'is_online': m.is_online,
            'last_seen': m.last_login.strftime('%Y-%m-%d %H:%M') if m.last_login else 'Never'
        })

        grand_total_records += record_count
        grand_total_sampling += total_sampling
        grand_total_patients += total_patients
        grand_total_moto_fee += total_moto
        grand_total_far_away_fee += total_far

    return render_template('admin_report.html',
                           member_data=member_data,
                           grand_total_records=grand_total_records,
                           grand_total_sampling=grand_total_sampling,
                           grand_total_patients=grand_total_patients,
                           grand_total_moto_fee=grand_total_moto_fee,
                           grand_total_far_away_fee=grand_total_far_away_fee,
                           total_members=len(members),
                           today=cambodia_today())


@app.route('/admin/report/print')
@admin_required
def admin_report_print():
    members = Member.query.order_by(Member.full_name.asc()).all()
    member_data = []
    grand_total_records = 0
    grand_total_sampling = 0
    grand_total_patients = 0
    grand_total_moto_fee = 0
    grand_total_far_away_fee = 0

    for m in members:
        record_count = CollectionRecord.query.filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).count()
        total_sampling = db.session.query(func.sum(CollectionRecord.total_sampling)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_patients = db.session.query(func.sum(CollectionRecord.patient_count)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_moto = db.session.query(func.sum(CollectionRecord.moto_fee)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        total_far = db.session.query(func.sum(CollectionRecord.far_away_fee)).filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).scalar() or 0
        last_record = CollectionRecord.query.filter(
            CollectionRecord.member_id == m.id,
            EXCLUDE_FEE_ONLY
        ).order_by(CollectionRecord.date.desc()).first()
        last_record_date = last_record.date.strftime('%Y-%m-%d') if last_record else 'Never'

        member_data.append({
            'id': m.id,
            'username': m.username,
            'full_name': m.full_name,
            'email': m.email or '-',
            'phone': m.phone or '-',
            'role': 'Admin' if m.is_admin else 'Member',
            'status': 'Active' if m.is_active else 'Inactive',
            'record_count': record_count,
            'total_sampling': total_sampling,
            'total_patients': total_patients,
            'total_moto_fee': total_moto,
            'total_far_away_fee': total_far,
            'last_record': last_record_date,
            'profile_picture': m.profile_picture,
            'created_at': m.created_at.strftime('%Y-%m-%d') if m.created_at else 'N/A',
            'is_online': m.is_online,
            'last_seen': m.last_login.strftime('%Y-%m-%d %H:%M') if m.last_login else 'Never'
        })

        grand_total_records += record_count
        grand_total_sampling += total_sampling
        grand_total_patients += total_patients
        grand_total_moto_fee += total_moto
        grand_total_far_away_fee += total_far

    return render_template('admin_report_print.html',
                           member_data=member_data,
                           grand_total_records=grand_total_records,
                           grand_total_sampling=grand_total_sampling,
                           grand_total_patients=grand_total_patients,
                           grand_total_moto_fee=grand_total_moto_fee,
                           grand_total_far_away_fee=grand_total_far_away_fee,
                           total_members=len(members),
                           today=cambodia_today(),
                           session=session)


# ==================== API ENDPOINTS ====================
@app.route('/api/request_reset', methods=['POST'])
def api_request_reset():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided'}), 400

    fullname = data.get('fullname', '').strip()
    username = data.get('username', '').strip()
    phone = data.get('phone', '').strip()
    code = data.get('code', '').strip()
    reason = data.get('reason', '').strip()

    errors = {}

    if not fullname:
        errors['fullname'] = 'Full name is required.'
    if not username:
        errors['username'] = 'Username is required.'
    if not phone:
        errors['phone'] = 'Phone number is required.'
    if not code:
        errors['code'] = 'Request code is required.'
    if not reason or len(reason) < 10:
        errors['reason'] = 'Reason must be at least 10 characters.'

    if errors:
        return jsonify({'success': False, 'errors': errors}), 400

    member = Member.query.filter_by(username=username).first()
    if not member:
        return jsonify({'success': False, 'errors': {'username': 'No account found with this username.'}}), 404

    phone_input = ''.join(filter(str.isdigit, phone))
    phone_db = ''.join(filter(str.isdigit, member.phone)) if member.phone else ''
    if not phone_db or phone_input != phone_db:
        return jsonify({'success': False, 'errors': {'phone': 'Phone number does not match our records.'}}), 400

    username_matches = (member.username.lower() == username.lower())
    name_matches = (member.full_name.lower() == fullname.lower())
    if not username_matches and not name_matches:
        return jsonify({'success': False, 'errors': {'fullname': 'Full name does not match our records.'}}), 400

    existing = PasswordResetRequest.query.filter(
        PasswordResetRequest.request_code == code,
        PasswordResetRequest.status.in_(['Pending', 'Approved'])
    ).first()
    if existing:
        return jsonify({'success': False, 'errors': {
            'code': 'This request code is already in use (pending or approved). If rejected, you can reuse it.'}}), 400

    try:
        reset_request = PasswordResetRequest(
            member_id=member.id,
            fullname=fullname,
            username=username,
            phone=phone if phone else None,
            request_code=code,
            reason=reason,
            status='Pending'
        )
        db.session.add(reset_request)
        db.session.commit()
        add_system_log(None, 'Password Reset Request',
                       f'User {username} requested password reset (Code: {code})')
        return jsonify(
            {'success': True, 'message': '✅ Password reset request submitted. Please wait for admin approval.'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Database error: {str(e)}'}), 500


@app.route('/api/check_code', methods=['POST'])
def check_code():
    data = request.get_json()
    if not data:
        return jsonify({'exists': False, 'message': 'No data provided'}), 400

    code = data.get('code', '').strip()
    if not code:
        return jsonify({'exists': False, 'message': 'No code provided'}), 400

    existing = PasswordResetRequest.query.filter(
        PasswordResetRequest.request_code == code,
        PasswordResetRequest.status.in_(['Pending', 'Approved'])
    ).first()

    if existing:
        return jsonify({
            'exists': True,
            'message': 'This request code is already in use (pending or approved). Please use a different code or contact admin.'
        })
    else:
        return jsonify({'exists': False, 'message': 'Code available – you can use it'})


@app.route('/api/check_reset_status', methods=['POST'])
def check_reset_status():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided'}), 400

    code = data.get('request_code', '').strip()
    if not code:
        return jsonify({'success': False, 'message': 'No code provided'}), 400

    req = PasswordResetRequest.query.filter_by(request_code=code).first()
    if not req:
        return jsonify({'success': False, 'message': 'Invalid request code.'}), 404

    if req.status == 'Pending':
        return jsonify({'success': True, 'status': 'pending', 'message': 'Your request is still pending admin review.'})

    if req.status == 'Rejected':
        return jsonify({'success': True, 'status': 'rejected',
                        'message': 'Your request was rejected. Please contact admin or submit a new request.'})

    if req.status == 'Approved':
        if req.admin_note and 'password_retrieved' in req.admin_note:
            return jsonify({'success': True, 'status': 'approved', 'retrieved': True,
                            'message': 'This password has already been retrieved. If you need help, please submit a new request.'})
        else:
            if req.admin_note and req.admin_note.startswith('new_password:'):
                new_pass = req.admin_note.split(':', 1)[1]
                req.admin_note = 'password_retrieved'
                db.session.commit()
                return jsonify({'success': True, 'status': 'approved', 'retrieved': False,
                                'message': 'Your password has been reset.', 'new_password': new_pass})
            else:
                return jsonify({'success': True, 'status': 'approved', 'retrieved': True,
                                'message': 'Password already retrieved. If you need help, contact admin.'})

    return jsonify({'success': False, 'message': 'Unknown status.'})


# ==================== BACKGROUND TASKS ====================
def cleanup_offline_users():
    """Background task to mark users as offline if they haven't been active"""
    with app.app_context():
        try:
            timeout_minutes = 5
            cutoff = cambodia_now() - timedelta(minutes=timeout_minutes)

            inactive_users = Member.query.filter(
                Member.is_online == True,
                Member.last_login < cutoff
            ).all()

            for user in inactive_users:
                user.is_online = False
                broadcast_status_update(user.id, False, {
                    'full_name': user.full_name,
                    'username': user.username
                })

            if inactive_users:
                db.session.commit()
                print(f"🔄 Marked {len(inactive_users)} inactive users as offline")
        except Exception as e:
            print(f"❌ Cleanup error: {e}")
            db.session.rollback()


def start_background_cleanup():
    """Start the background cleanup thread"""
    def run_cleanup():
        while True:
            time_module.sleep(60)
            cleanup_offline_users()

    thread = threading.Thread(target=run_cleanup, daemon=True)
    thread.start()
    print("✅ Background cleanup thread started")


# ==================== TEARDOWN ====================
@app.teardown_appcontext
def shutdown_session(exception=None):
    try:
        db.session.remove()
    except:
        pass


# ==================== ERROR HANDLERS ====================
@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(413)
def request_entity_too_large(e):
    flash('❌ File too large. Maximum size is 500KB. Please use a smaller image.', 'danger')
    return redirect(request.referrer or url_for('dashboard'))


@app.errorhandler(500)
def server_error(e):
    db.session.rollback()
    return render_template('500.html'), 500


# ==================== CONTEXT PROCESSOR ====================
@app.context_processor
def utility_processor():
    pending_resets = 0
    if 'user_id' in session and session.get('is_admin'):
        try:
            pending_resets = PasswordResetRequest.query.filter_by(status='Pending').count()
        except:
            pass
    return dict(pending_resets=pending_resets)


# ==================== INIT DB ====================
def create_indexes():
    """Create database indexes for better performance"""
    try:
        if os.environ.get('DATABASE_URL'):
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_collection_member_date ON collection_record (member_id, date DESC)",
                "CREATE INDEX IF NOT EXISTS idx_collection_date ON collection_record (date DESC)",
                "CREATE INDEX IF NOT EXISTS idx_collection_member_id ON collection_record (member_id)",
                "CREATE INDEX IF NOT EXISTS idx_member_username ON member (username)",
                "CREATE INDEX IF NOT EXISTS idx_member_is_online ON member (is_online)",
                "CREATE INDEX IF NOT EXISTS idx_member_is_active ON member (is_active)",
                "CREATE INDEX IF NOT EXISTS idx_log_timestamp ON system_log (timestamp DESC)",
                "CREATE INDEX IF NOT EXISTS idx_log_user_id ON system_log (user_id)",
                "CREATE INDEX IF NOT EXISTS idx_reset_status ON password_reset_request (status)",
                "CREATE INDEX IF NOT EXISTS idx_reset_request_code ON password_reset_request (request_code)"
            ]
            for idx in indexes:
                try:
                    db.session.execute(text(idx))
                except Exception as e:
                    print(f"⚠️ Could not create index: {e}")
            db.session.commit()
            print("✅ Indexes created/verified")
        else:
            print("📁 SQLite - indexes will be created on table creation")
    except Exception as e:
        print(f"⚠️ Could not create indexes: {e}")
        db.session.rollback()


def init_db():
    with app.app_context():
        # Reset all users to offline on startup
        try:
            if os.environ.get('DATABASE_URL'):
                db.session.execute(text("UPDATE member SET is_online = false"))
                db.session.commit()
                print("✅ All users set to offline on startup (PostgreSQL)")
            else:
                members = Member.query.all()
                for member in members:
                    member.is_online = False
                db.session.commit()
                print("✅ All users set to offline on startup (SQLite)")
        except Exception as e:
            print(f"ℹ️ No users to reset or table doesn't exist yet: {e}")

        db.create_all()
        print("✅ All tables created/updated!")

        # Create indexes
        create_indexes()

        # Add fee columns if they don't exist
        try:
            inspector = inspect(db.engine)
            if 'collection_record' in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns('collection_record')]
                if 'moto_fee' not in columns:
                    db.session.execute(text("ALTER TABLE collection_record ADD COLUMN moto_fee INTEGER DEFAULT 1000"))
                    db.session.commit()
                    print("✅ Added moto_fee column")
                if 'far_away_fee' not in columns:
                    db.session.execute(
                        text("ALTER TABLE collection_record ADD COLUMN far_away_fee INTEGER DEFAULT 5000"))
                    db.session.commit()
                    print("✅ Added far_away_fee column")
        except Exception as e:
            print(f"⚠️ Could not add fee columns: {e}")
            db.session.rollback()

        # Create default profile picture
        default_pic = os.path.join(app.config['UPLOAD_FOLDER'], 'default.png')
        if not os.path.exists(default_pic):
            try:
                img = Image.new('RGB', (300, 300), color='#2b5d8d')
                img.save(default_pic)
                print("✅ Default profile picture created")
            except Exception as e:
                print(f"⚠️ Could not create default profile picture: {e}")

        # Create admin account if it doesn't exist
        if not Member.query.filter_by(username='admin').first():
            admin = Member(username='admin', full_name='System Admin', email='admin@cammedlab.com', is_admin=True)
            admin.set_password('admin123')
            db.session.add(admin)
            db.session.commit()
            print("✅ Admin account created!")

        # Create sample members
        members_data = [
            ('member1', 'Ahmad bin Ali'),
            ('member2', 'Bala Subramaniam'),
            ('member3', 'Chen Wei Ming'),
            ('member4', 'David Raj'),
            ('member5', 'Emily Tan'),
            ('member6', 'Fatimah binti Yusof'),
        ]
        created_count = 0
        for username, full_name in members_data:
            if not Member.query.filter_by(username=username).first():
                m = Member(username=username, full_name=full_name, is_admin=False, is_active=True)
                m.set_password('pass123')
                db.session.add(m)
                created_count += 1
        if created_count > 0:
            db.session.commit()
            print(f"✅ {created_count} new members created!")

        # Clear cache on startup
        cache_clear()
        print("✅ Cache cleared on startup")

        print("✅ Database ready!")
        print(f"🕐 Cambodia Time (UTC+7): {cambodia_now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📁 Profile storage: {'Cloudinary (permanent)' if USE_CLOUDINARY else 'Local file system'}")


# ==================== STARTUP ====================
init_db()
start_background_cleanup()

# For Vercel
app = app

if __name__ == '__main__':
    print("\n" + "=" * 50)
    print("🔬 CAMMED LAB - CMS (Optimized)")
    print("=" * 50)
    print(f"🌍 Environment: {'Production' if os.environ.get('VERCEL') else 'Development'}")
    print(f"🗄️  Database: {'PostgreSQL' if os.environ.get('DATABASE_URL') else 'SQLite'}")
    print(f"🖼️  Profile Storage: {'Cloudinary (permanent)' if USE_CLOUDINARY else 'Local file system'}")
    print(f"⚡ Performance Optimizations: Enabled")
    print("=" * 50)
    print("Server: http://127.0.0.1:5000")
    print("Admin: admin / admin123")
    print("Members: member1-6 / pass123")
    print("Timezone: Cambodia (UTC+7)")
    print("=" * 50 + "\n")
    app.run(debug=True, host='127.0.0.1', port=5000)
