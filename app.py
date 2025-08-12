from flask import Flask, render_template, redirect, url_for, request, flash, send_file, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
import os
from functools import wraps
from flask_wtf import CSRFProtect  # <- evita importarlo dos veces
from sqlalchemy.engine.url import make_url
import sys

# -----------------------------
# Inicialización de la aplicación
# -----------------------------
app = Flask(__name__)
# Usa una SECRET_KEY fija desde entorno en producción para no invalidar sesiones en cada deploy
app.secret_key = os.getenv('SECRET_KEY', 'cambia-esto-en-produccion')
csrf = CSRFProtect(app)

# -----------------------------
# Configuración de directorios base
# -----------------------------
basedir = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
LOCAL_DATA_DIR = os.path.join(basedir, 'data')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

# -----------------------------
# Helper: normalizar URI de Postgres y forzar SSL
# -----------------------------

def _ensure_postgres_uri(uri: str) -> str:
    if not uri:
        return uri

    # Normaliza postgres:// -> postgresql://
    fixed = uri.replace('postgres://', 'postgresql://', 1)

    # Elegir driver según Python: 3.13 usa psycopg (v3), 3.12 o menor usa psycopg2
    prefer_driver = 'psycopg' if sys.version_info >= (3, 13) else 'psycopg2'

    # Limpia cualquier driver previo y coloca el preferido
    fixed = fixed.replace('postgresql+psycopg2://', 'postgresql://', 1)
    fixed = fixed.replace('postgresql+psycopg://', 'postgresql://', 1)
    if fixed.startswith('postgresql://'):
        fixed = fixed.replace('postgresql://', f'postgresql+{prefer_driver}://', 1)

    # sslmode=require si falta
    if 'sslmode=' not in fixed:
        fixed += ('&' if '?' in fixed else '?') + 'sslmode=require'

    return fixed


# -----------------------------
# Configuración mejorada de la base de datos con persistencia garantizada
# -----------------------------
def configure_database() -> str:
    """
    Producción (Render):
      - Si hay DATABASE_URL -> usa Postgres (con driver correcto + sslmode=require)
      - Si NO hay DATABASE_URL pero existe PORT (señal de Render) -> ERROR (no usar SQLite)
    Desarrollo local:
      - Usa SQLite persistente en ./data/database.db
    """
    db_url = os.environ.get('DATABASE_URL')

    # 1) Producción con DATABASE_URL
    if db_url:
        print("Configuración: PostgreSQL en producción (Render)")
        uri = _ensure_postgres_uri(db_url)
        print(f"SQLAlchemy URI final => {uri}")
        return uri

    # 2) Estamos en Render (si hay PORT) pero sin DATABASE_URL -> no permitir SQLite
    if os.environ.get('PORT'):
        raise RuntimeError(
            "DATABASE_URL no está definida en el entorno de Render. "
            "Ve a Settings → Environment y agrega DATABASE_URL con la cadena de conexión de Postgres."
        )

    # 3) Desarrollo local -> SQLite
    db_path = os.path.join(LOCAL_DATA_DIR, 'database.db')
    print(f"Configuración: SQLite local con persistencia -> {db_path}")
    return f"sqlite:///{db_path}"

# -----------------------------
# Configuración principal de la aplicación
# -----------------------------
app.config.update(
    SQLALCHEMY_DATABASE_URI=configure_database(),
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SQLALCHEMY_ENGINE_OPTIONS={
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'pool_size': 10,
        'max_overflow': 5,
        'pool_timeout': 30,
    },
    UPLOAD_FOLDER=UPLOAD_FOLDER,
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16MB
    SESSION_COOKIE_SECURE=True,   # Render sirve HTTPS
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=2),
    SQLALCHEMY_ECHO=False,
    SQLALCHEMY_RECORD_QUERIES=False,
    PROPAGATE_EXCEPTIONS=True,
)

app.logger.info(f"DB URL -> {app.config['SQLALCHEMY_DATABASE_URI']}")
try:
    drv = make_url(app.config['SQLALCHEMY_DATABASE_URI']).drivername
    app.logger.info(f"DB driver -> {drv}")
except Exception as e:
    app.logger.warning(f"No pude parsear DB URL: {e}")


# 🔹 Desactivar cookie segura si estás en modo debug local (HTTP sin HTTPS)
if os.environ.get('FLASK_DEBUG') == 'True':
    app.config['SESSION_COOKIE_SECURE'] = False


# -----------------------------
# Inicialización de extensiones
# -----------------------------
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# Modelos de base de datos
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)

    @property
    def password(self):
        raise AttributeError('La contraseña no es un atributo legible')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

class Transferencia(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.String(20))
    banco = db.Column(db.String(50))
    banco_receptor = db.Column(db.String(50))
    monto = db.Column(db.Float)
    referencia = db.Column(db.String(100), unique=True)
    pedido = db.Column(db.String(100))
    factura = db.Column(db.String(100))
    registrado = db.Column(db.String(50))
    esta_registrado = db.Column(db.Boolean, default=False)
    concepto = db.Column(db.String(200))

class Venta(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date)
    concepto = db.Column(db.String(255))
    tipo = db.Column(db.String(100))
    subtipo = db.Column(db.String(100))
    cantidad = db.Column(db.Float)
    usuario = db.Column(db.String(100))
    
    codigo = db.Column(db.String(50))
    num = db.Column(db.String(50))
    no_fac = db.Column(db.String(50))
    no_nota = db.Column(db.String(50))
    cant = db.Column(db.Float)
    cve_age = db.Column(db.String(20))
    nom_cte = db.Column(db.String(100))
    rfc_cte = db.Column(db.String(30))
    des_mon = db.Column(db.String(20))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'csv', 'xlsx'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin:
            flash('No tienes permisos para acceder a esta página', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

@app.before_first_request
def initialize_database_if_needed():
    """Se ejecuta al primer request (por worker). Idempotente."""
    try:
        db.create_all()
        if User.query.count() == 0:
            admin = User(username='admin', is_admin=True)
            admin.password = os.getenv('ADMIN_PASSWORD', 'admin123')
            db.session.add(admin)
            db.session.commit()
            app.logger.info("Usuario admin creado automáticamente.")
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"Error inicializando la BD: {e}")


# Rutas de autenticación mejoradas
@app.route('/', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if User.query.count() == 0:
        return redirect(url_for('registro_usuario'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        
        if not username or not password:
            flash('Por favor ingrese usuario y contraseña', 'error')
            return redirect(url_for('login'))

        user = User.query.filter_by(username=username).first()
        
        if user and user.verify_password(password):
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('dashboard'))
        
        flash('Usuario o contraseña incorrectos', 'error')
    
    return render_template('login.html')

@app.route('/registro-usuario', methods=['GET', 'POST'])
def registro_usuario():
    if User.query.count() > 0 and not current_user.is_authenticated:
        flash("Ya existe un usuario registrado.")
        return redirect(url_for('login'))

    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        confirm_password = request.form.get('confirm_password')
        
        if not username or not password:
            flash('Todos los campos son requeridos', 'error')
        elif password != confirm_password:
            flash('Las contraseñas no coinciden', 'error')
        elif User.query.filter_by(username=username).first():
            flash('El usuario ya existe', 'error')
        else:
            nuevo = User(username=username, is_admin=(User.query.count() == 0))
            nuevo.password = password
            db.session.add(nuevo)
            db.session.commit()
            flash('Usuario creado exitosamente. Ahora puedes iniciar sesión.', 'success')
            return redirect(url_for('login'))
    
    return render_template('registro_usuario.html')

@app.route('/admin/usuarios')
@login_required
@admin_required
def admin_usuarios():
    usuarios = User.query.all()
    return render_template('admin_usuarios.html', usuarios=usuarios)

@app.route('/admin/usuarios/nuevo', methods=['GET', 'POST'])
@login_required
@admin_required
def nuevo_usuario():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        confirm_password = request.form.get('confirm_password')
        is_admin = 'is_admin' in request.form

        if not username or not password:
            flash('Todos los campos son requeridos', 'error')
        elif password != confirm_password:
            flash('Las contraseñas no coinciden', 'error')
        elif User.query.filter_by(username=username).first():
            flash('El usuario ya existe', 'error')
        else:
            nuevo = User(username=username, is_admin=is_admin)
            nuevo.password = password
            db.session.add(nuevo)
            db.session.commit()
            flash('Usuario creado exitosamente.', 'success')
            return redirect(url_for('admin_usuarios'))

    return render_template('nuevo_usuario.html')


@app.route('/admin/usuarios/editar/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_required
def editar_usuario(id):
    usuario = User.query.get_or_404(id)
    
    if request.method == 'POST':
        try:
            # Debug: Mostrar datos recibidos
            app.logger.debug(f"Datos recibidos: {request.form}")
            
            # Validación de datos
            username = request.form.get('username', '').strip()
            if not username:
                flash('El nombre de usuario es requerido', 'error')
                return redirect(url_for('editar_usuario', id=id))
            
            # Verificar si el username ya existe
            if User.query.filter(User.username == username, User.id != id).first():
                flash('Este nombre de usuario ya está en uso', 'error')
                return redirect(url_for('editar_usuario', id=id))
            
            # Actualizar datos
            usuario.username = username
            usuario.is_admin = 'is_admin' in request.form
            
            # Manejar cambio de contraseña
            new_password = request.form.get('password', '').strip()
            if new_password:
                confirm_password = request.form.get('confirm_password', '').strip()
                
                if len(new_password) < 6:
                    flash('La contraseña debe tener al menos 6 caracteres', 'error')
                    return redirect(url_for('editar_usuario', id=id))
                
                if new_password != confirm_password:
                    flash('Las contraseñas no coinciden', 'error')
                    return redirect(url_for('editar_usuario', id=id))
                
                usuario.password = new_password
            
            db.session.commit()
            flash('Usuario actualizado correctamente', 'success')
            return redirect(url_for('admin_usuarios'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error al actualizar el usuario: {str(e)}', 'error')
            app.logger.error(f"Error al editar usuario {id}: {str(e)}")
            return redirect(url_for('editar_usuario', id=id))
    
    return render_template('editar_usuario.html', usuario=usuario)

@app.route('/admin/usuarios/eliminar/<int:id>', methods=['POST'])
@login_required
@admin_required
def eliminar_usuario(id):
    try:
        usuario = User.query.get_or_404(id)
        
        # Verificación de seguridad adicional
        if usuario.id == current_user.id:
            flash('No puedes eliminar tu propio usuario.', 'error')
            return redirect(url_for('admin_usuarios'))

        db.session.delete(usuario)
        db.session.commit()
        flash('Usuario eliminado correctamente.', 'success')
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"Error al eliminar usuario {id}: {str(e)}")
        flash('Ocurrió un error al eliminar el usuario.', 'error')
    
    return redirect(url_for('admin_usuarios'))


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Has cerrado sesión correctamente', 'info')
    return redirect(url_for('login'))


@app.route('/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar(id):
    transferencia = Transferencia.query.get_or_404(id)
    if request.method == 'POST':
        transferencia.pedido = request.form['pedido']
        transferencia.factura = request.form['factura']
        transferencia.esta_registrado = 'esta_registrado' in request.form
        transferencia.registrado = request.form['registrado']
        db.session.commit()
        flash("Transferencia actualizada correctamente.")
        return redirect(url_for('dashboard'))
    return render_template('editar.html', t=transferencia)

@app.route('/eliminar/<int:id>', methods=['POST'])
@login_required
def eliminar(id):
    t = Transferencia.query.get_or_404(id)
    db.session.delete(t)
    db.session.commit()
    flash("Transferencia eliminada correctamente.")
    return redirect(url_for('dashboard'))

@app.route('/eliminar-todas', methods=['POST'])
@login_required
def eliminar_todas():
    Transferencia.query.delete()
    db.session.commit()
    flash('Se eliminaron todas las transferencias.')
    return redirect(url_for('dashboard'))

@app.route('/toggle-registrado/<int:id>', methods=['POST'])
@login_required
def toggle_registrado(id):
    t = Transferencia.query.get_or_404(id)
    t.esta_registrado = 'esta_registrado' in request.form
    db.session.commit()
    return redirect(url_for('dashboard'))

@app.route('/registro', methods=['GET', 'POST'])
@login_required
def registro():
    if request.method == 'POST':
        ref = request.form['referencia']
        existente = Transferencia.query.filter_by(referencia=ref).first()
        if existente:
            flash("Ya existe una transferencia con esa referencia.", 'error')
        else:
            t = Transferencia(
                fecha=request.form['fecha'],
                banco=request.form['banco'],
                monto=float(str(request.form['monto']).replace(',', '').strip() or 0),
                referencia=ref,
                pedido=request.form['pedido'],
                factura=request.form['factura'],
                registrado=request.form['registrado'],
                esta_registrado='esta_registrado' in request.form,
                concepto=request.form.get('concepto', '')  
            )
            db.session.add(t)
            db.session.commit()
            flash("Transferencia registrada correctamente.", 'success')
            return redirect(url_for('dashboard'))
    
    # Pasa datetime al contexto de la plantilla
    return render_template('registro.html', datetime=datetime)

@app.route('/subir-archivo', methods=['GET', 'POST'])
@login_required
def subir_archivo():
    resultados = None
    if request.method == 'POST':
        archivo = request.files['archivo']
        if archivo and allowed_file(archivo.filename):
            filename = secure_filename(archivo.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            archivo.save(filepath)

            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(filepath, dtype={'fecha': str})
                else:
                    df = pd.read_excel(filepath)

                # Normaliza encabezados a minúsculas y sin espacios laterales
                df.columns = [col.strip().lower() for col in df.columns]
                print("Encabezados normalizados:", df.columns)

                # Renombrados útiles
                if 'banco participante' in df.columns:
                    df.rename(columns={'banco participante': 'banco_participante'}, inplace=True)

                # Fecha cruda
                if 'fecha' in df.columns:
                    df.rename(columns={'fecha': 'fecha_raw'}, inplace=True)
                elif 'fecha de movimiento' in df.columns:
                    df.rename(columns={'fecha de movimiento': 'fecha_raw'}, inplace=True)

                # --- Ubicar columna Cargo/Abono (obligatoria para este flujo) ---
                posibles_signos = [
                    'cargo/abono', 'cargo_abono', 'cargo-abono',
                    'cargo o abono', 'abono/cargo', 'signo', 'cargoabono'
                ]

                sign_col = next((c for c in posibles_signos if c in df.columns), None)
                if not sign_col:
                    flash("El archivo debe contener la columna 'Cargo/Abono' (por ejemplo: Cargo/Abono, cargo_abono, cargo-abono).", 'error')
                    return redirect(request.url)

                nuevas = 0
                duplicadas = 0
                saltadas_no_ingreso = 0

                for _, row in df.iterrows():
                    # Acepta "+" y también "ABONO" (en cualquier combinación de mayúsculas/minúsculas)
                    signo_raw = row.get(sign_col, '')
                    signo = str(signo_raw).strip().lower()  # "ABONO" -> "abono", " + " -> "+"

                    if signo not in ('+', 'abono'):
                        saltadas_no_ingreso += 1
                        continue


                    # 2) Evitar duplicados por referencia
                    referencia = str(row.get('referencia', '')).strip()
                    if not referencia or Transferencia.query.filter_by(referencia=referencia).first():
                        duplicadas += 1
                        continue

                    # 3) Procesamiento de fecha
                    raw_fecha = row.get('fecha_raw', '')
                    fecha = None
                    try:
                        if pd.isna(raw_fecha) or not str(raw_fecha).strip():
                            raise ValueError("Fecha vacía")

                        if isinstance(raw_fecha, (int, float)):
                            raw_fecha = str(int(raw_fecha)).zfill(8)
                        else:
                            raw_fecha = str(raw_fecha).strip().replace("'", "").replace('"', '').replace('‘', '').replace('’', '')

                        formatos = [
                            '%d%m%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
                            '%Y%m%d', '%d-%m-%Y', '%Y/%m/%d',
                            '%d.%m.%Y', '%d-%b-%y', '%d-%B-%Y', '%m/%d/%y',
                        ]
                        for fmt in formatos:
                            try:
                                fecha = pd.to_datetime(raw_fecha, format=fmt).date().isoformat()
                                break
                            except Exception:
                                continue
                        if not fecha:
                            fecha = pd.to_datetime(raw_fecha).date().isoformat()
                    except Exception as e:
                        print(f"Error al convertir fecha: {raw_fecha} → {e}")
                        fecha = datetime.now().date().isoformat()

                    # 4) Banco emisor
                    banco = str(row.get('banco_participante', '')).strip() or 'Desconocido'

                    # 5) Banco receptor desde cuenta/clabe
                    def _s(v):
                        s = '' if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
                        s = s.strip().replace("'", "")
                        return '' if s.lower() == 'nan' else s

                    cuenta = _s(row.get('cuenta', ''))
                    clabe  = _s(row.get('clabe', ''))

                    cuenta_completa = cuenta or clabe
                    banco_receptor = 'Desconocido'

                    cuentas_destino = {
                        'BBVA': ['0185077915', '012420001850779158'],
                        'BANAMEX': ['53700061612', '002420053700616125'],
                        'SANTANDER': ['5150036891', '014420515003689123', '51500368912']
                    }

                    for banco_nombre, cuentas in cuentas_destino.items():
                        for c in cuentas:
                            if cuenta_completa.endswith(c) or cuenta_completa == c:
                                banco_receptor = banco_nombre
                                break
                        if banco_receptor != 'Desconocido':
                            break

                    # 6) Monto y otros campos
                    monto = float(row.get('importe', 0) or 0)
                    pedido = str(row.get('pedido', '')).strip()
                    factura = str(row.get('factura', '')).strip()
                    concepto = str(row.get('concepto', '')).strip()

                    # 7) Crear y guardar
                    t = Transferencia(
                        fecha=fecha,
                        banco=banco,
                        banco_receptor=banco_receptor,
                        monto=monto,
                        referencia=referencia,
                        pedido=pedido,
                        factura=factura,
                        registrado=current_user.username,
                        esta_registrado=False,
                        concepto=concepto
                    )

                    db.session.add(t)
                    nuevas += 1

                db.session.commit()
                flash(
                    f'Se procesó el archivo. Nuevas: {nuevas}, '
                    f'Duplicadas/omitidas por referencia: {duplicadas}, '
                    f'Omitidas por no ser ingreso (+/ABONO): {saltadas_no_ingreso}',
                    'success'
                )
                return redirect(url_for('dashboard'))

            except Exception as e:
                db.session.rollback()
                flash(f'Ocurrió un error al procesar el archivo: {e}', 'error')
                return redirect(request.url)
        else:
            flash('Archivo no válido. Solo se permiten .csv o .xlsx', 'error')

    return render_template('subir_archivo.html')



@app.route('/transformar_excel', methods=['POST'])
@login_required
def transformar_excel():
    archivo = request.files.get('archivo_excel')
    if not archivo:
        flash("No se subió ningún archivo", "error")
        return redirect(url_for('dashboard'))

    df = pd.read_excel(archivo)
    df.columns = [col.strip() for col in df.columns]

    columnas_requeridas = [
        "Cve_factu", "No_fac", "Num", "Fecha", "Cantidad", "Tipo",
        "No_nota", "Subtipo", "Cant", "Cve_age", "Nom_cte", "Rfc_cte", "Des_mon"
    ]
    if not all(col in df.columns for col in columnas_requeridas):
        flash("El archivo no contiene todas las columnas requeridas", "error")
        return redirect(url_for('dashboard'))

    # Convertir fecha a solo la parte de fecha (sin hora)
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors='coerce').dt.date

    # Crear columna combinada sin espacios ni encabezado
    df.insert(0, '', df['Cve_factu'].astype(str).str.strip() + df['No_fac'].astype(str).str.strip())

    # Limpiar campos para evitar errores por espacios
    columnas_a_limpieza = ['Nom_cte', 'Rfc_cte', 'Des_mon']
    for col in columnas_a_limpieza:
        df[col] = df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Reordenar columnas
    columnas_orden = [
        '', 'Num', 'No_fac', 'Fecha', 'Cantidad', 'Tipo',
        'No_nota', 'Subtipo', 'Cant', 'Cve_age',
        'Nom_cte', 'Rfc_cte', 'Des_mon'
    ]
    df_final = df[columnas_orden].copy()

    # Filtrar registros con "DEPOSITO CLI" en Subtipo
    df_final = df_final[~df_final["Subtipo"].astype(str).str.contains("DEPOSITO CLI", na=False)]

    # Crear libro de Excel
    wb = Workbook()
    ws = wb.active

    # Encabezados (con columna inicial sin título)
    encabezados = [
        "", "Num", "No_fac", "Fecha", "Cantidad", "Tipo",
        "No_nota", "Subtipo", "Cant", "Cve_age",
        "Nom_cte", "Rfc_cte", "Des_mon"
    ]
    ws.append(encabezados)

    # Agregar los datos sin encabezado automático
    for row in dataframe_to_rows(df_final, index=False, header=False):
        ws.append(row)

    # Estilo de encabezado
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center')

    # Ajustar ancho de columnas
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_length + 2

    # Forzar primera columna a formato texto
    for row in ws.iter_rows(min_row=2, min_col=1, max_col=1):
        for cell in row:
            cell.number_format = '@'

    # Guardar archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"excel_transformado_{timestamp}.xlsx"
    output_path = os.path.join("uploads", filename)
    wb.save(output_path)

    return send_file(output_path, as_attachment=True)


@app.route('/transformar_excel_clasificado', methods=['POST'])
@login_required
def transformar_excel_clasificado():
    archivo = request.files.get('archivo_excel')
    if not archivo:
        flash("No se subió ningún archivo", "error")
        return redirect(url_for('dashboard'))

    df = pd.read_excel(archivo)
    df.columns = [col.strip() for col in df.columns]

    columnas_requeridas = [
        "Cve_factu", "No_fac", "Num", "Fecha", "Cantidad", "Tipo",
        "No_nota", "Subtipo", "Cant", "Cve_age", "Nom_cte", "Rfc_cte", "Des_mon"
    ]
    if not all(col in df.columns for col in columnas_requeridas):
        flash("El archivo no contiene todas las columnas requeridas", "error")
        return redirect(url_for('dashboard'))

    df.insert(0, '', df['Cve_factu'].astype(str).str.strip() + df['No_fac'].astype(str).str.strip())

    for col in ['Nom_cte', 'Rfc_cte', 'Des_mon']:
        df[col] = df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce').dt.strftime('%d/%m/%Y')

    def clasificar(row):
        subtipo = str(row.get('Subtipo', '')).upper()
        if "CAJA 5MAYO" in subtipo:
            return "CAJA 5 MAYO"
        elif "SLORENZ" in subtipo:
            return "SAN LORENZO"
        elif "TEC" in subtipo:
            return "TECNOLÓGICO"
        elif "PENIN" in subtipo:
            return "PENINSULA"
        else:
            return "OTROS MOVIMIENTOS"

    df['__clasificacion_temp'] = df.apply(clasificar, axis=1)

    columnas_orden = [
        '', 'Num', 'No_fac', 'Fecha', 'Cantidad', 'Tipo',
        'No_nota', 'Subtipo', 'Cant', 'Cve_age',
        'Nom_cte', 'Rfc_cte', 'Des_mon'
    ]
    df_final = df[columnas_orden + ['__clasificacion_temp']].copy()

    df_final = df_final[~df_final["Subtipo"].astype(str).str.contains("DEPOSITO CLI", na=False)]

    df_final["Cant"] = pd.to_numeric(df_final["Cant"], errors='coerce').fillna(0)
    df_final["Cantidad"] = pd.to_numeric(df_final["Cantidad"], errors='coerce').fillna(0)
    df_final["Tipo"] = df_final["Tipo"].astype(str).str.strip().str.upper()

    total_efectivo = df_final[df_final["Tipo"] == "EFECTIVO"]["Cantidad"].sum()

    subtotales = {}
    for subtipo in ["CAJA 5 MAYO", "SAN LORENZO", "TECNOLÓGICO", "PENINSULA"]:
        monto = df_final[df_final["__clasificacion_temp"] == subtipo]["Cant"].sum()
        subtotales[subtipo] = monto

    fila_resumen = [
        f"${total_efectivo:,.2f}",
        "TOTAL EFECTIVO",
        "OTROS MOVIMIENTOS"
    ]
    for subtipo in ["CAJA 5 MAYO", "SAN LORENZO", "TECNOLÓGICO", "PENINSULA"]:
        fila_resumen.append(subtipo)
        fila_resumen.append(f"${subtotales[subtipo]:,.2f}")

    wb = Workbook()
    ws = wb.active

    # Agrega encabezado
    ws.append(columnas_orden)

    # Agrega filas de datos
    for row in dataframe_to_rows(df_final[columnas_orden], index=False, header=False):
        ws.append(row)

    # Estilo encabezado
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center')

    # Ajustar ancho columnas
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_length + 2

    # Agregar fila resumen en la última fila
    ws.append(fila_resumen)
    ultima_fila = ws.max_row
    azul_claro = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")

    for cell in ws[ultima_fila]:
        cell.fill = azul_claro
        cell.font = Font(bold=True)

    # Guardar archivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"excel_clasificado_{timestamp}.xlsx"
    output_path = os.path.join("uploads", filename)
    wb.save(output_path)

    return send_file(output_path, as_attachment=True)


@app.route('/dashboard')
@login_required
def dashboard():
    query = Transferencia.query

    # filtros de búsqueda
    referencia = request.args.get('referencia')
    if referencia:
        query = query.filter(Transferencia.referencia.contains(referencia))
    fecha = request.args.get('fecha')
    if fecha:
        query = query.filter(Transferencia.fecha == fecha)
    banco = request.args.get('banco')
    if banco:
        query = query.filter(Transferencia.banco == banco)
    banco_receptor = request.args.get('banco_receptor') 
    if banco_receptor:
        query = query.filter(Transferencia.banco_receptor == banco_receptor)

    transferencias = query.order_by(Transferencia.fecha.desc()).all()

    # 🔽 AQUÍ ES DONDE AGREGAS LA CLAVE COMBINADA
    ventas = Venta.query.order_by(Venta.fecha.desc()).all()
    for v in ventas:
        v.c = f"{v.tipo[:2].upper() if v.tipo else ''}{v.no_fac}"

    return render_template(
        'dashboard.html',
        name=current_user.username,
        transferencias=transferencias,
        ventas=ventas
    )


@app.route('/subir_ventas', methods=['POST'])
@login_required
def subir_ventas():
    archivo = request.files.get('archivo_excel')
    if not archivo:
        flash("No se subió ningún archivo", "error")
        return redirect(url_for('dashboard'))

    # Leer sin encabezado
    df = pd.read_excel(archivo, header=None)

    # Asignar encabezados personalizados (agrega 'codigo' al principio)
    columnas = [
        'codigo', 'Num', 'No_fac', 'Fecha', 'Cantidad', 'Tipo', 'No_nota', 'Subtipo',
        'Cant', 'Cve_age', 'Nom_cte', 'Rfc_cte', 'Des_mon'
    ]
    df.columns = columnas

    for _, fila in df.iterrows():
        # Validar campos requeridos
        if pd.isna(fila['Fecha']) or pd.isna(fila['Cant']):
            continue

        # Procesar fecha
        try:
            fecha = pd.to_datetime(fila['Fecha'], dayfirst=True, errors='raise').date()
        except Exception:
            continue

        # Procesar cantidad
        try:
            cantidad = float(fila['Cant'])
        except Exception:
            continue

        # Crear instancia de Venta
        venta = Venta(
            fecha=fecha,
            concepto=str(fila.get('codigo', '')).strip(),  # puedes cambiar a otro campo si lo prefieres
            tipo=str(fila.get('Tipo', '')).strip(),
            subtipo=str(fila.get('Subtipo', '')).strip(),
            cantidad=cantidad,
            usuario=current_user.username,  # ahora guarda el usuario autenticado

            codigo=str(fila.get('codigo', '')).strip(),
            num=str(fila.get('Num', '')).strip(),
            no_fac=str(fila.get('No_fac', '')).strip(),
            no_nota=str(fila.get('No_nota', '')).strip(),
            cant=fila.get('Cant', 0),
            cve_age=str(fila.get('Cve_age', '')).strip(),
            nom_cte=str(fila.get('Nom_cte', '')).strip(),
            rfc_cte=str(fila.get('Rfc_cte', '')).strip(),
            des_mon=str(fila.get('Des_mon', '')).strip()
        )

        # Evitar duplicados por fecha, código y cantidad
        existe = Venta.query.filter_by(
            fecha=fecha,
            codigo=venta.codigo,
            cantidad=venta.cantidad
        ).first()

        if not existe:
            db.session.add(venta)

    db.session.commit()
    flash("Archivo de ventas cargado correctamente", "success")
    return redirect(url_for('dashboard') + '#tab-ventas')

@app.route('/ventas')
@login_required
def filtrar_ventas():
    fecha_str = request.args.get('fecha')
    if fecha_str:
        # Pasa la fecha al dashboard como query param (si ahí filtras por fecha de ventas, ajústalo)
        return redirect(url_for('dashboard', fecha=fecha_str) + '#tab-ventas')
    return redirect(url_for('dashboard') + '#tab-ventas')


# Manejo de errores 500
@app.errorhandler(500)
def handle_500(error):
    db.session.rollback()  # Previene bloqueos de la base de datos
    return render_template('500.html'), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False') == 'True'
    app.run(host='0.0.0.0', port=port, debug=debug)

