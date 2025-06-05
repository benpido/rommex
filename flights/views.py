from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.http import FileResponse
from django.conf import settings
import os
from .dashboard import run_etl

@login_required
def dashboard_view(request):
    return render(request, 'dashboard.html')

@login_required
def refresh_data(request):
    try:
        run_etl()
        message = 'Datos actualizados correctamente.'
    except Exception as e:
        message = f'Error al actualizar: {e}'
    return render(request, 'dashboard.html', {'message': message})

@login_required
def download_parquet(request):
    path = settings.PARQUET_FINAL
    if os.path.exists(path):
        return FileResponse(
            open(path, "rb"),
            as_attachment=True,
            filename=os.path.basename(path),
        )
    return render(request, 'dashboard.html', {'error': 'Archivo no encontrado.'})
    