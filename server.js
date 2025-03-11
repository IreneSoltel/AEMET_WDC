const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Habilitar CORS para todas las rutas
app.use(cors());

// Servir archivos estáticos
app.use(express.static(path.join(__dirname)));

// Proxy para la API de AEMET
app.get('/proxy/aemet', async (req, res) => {
  try {
    const apiUrl = req.query.url;
    const apiKey = req.query.apiKey;
    
    if (!apiUrl || !apiKey) {
      return res.status(400).json({ error: 'Se requiere URL y API key' });
    }
    
    console.log(`Proxy - Realizando petición a: ${apiUrl}`);
    
    const response = await axios.get(apiUrl, {
      headers: {
        'api_key': apiKey
      }
    });
    
    console.log('Proxy - Respuesta recibida:', response.status);
    
    // Enviar la respuesta completa al cliente
    return res.json(response.data);
  } catch (error) {
    console.error('Proxy - Error:', error.message);
    
    // Manejar errores de respuesta
    if (error.response) {
      return res.status(error.response.status || 500).json({
        error: 'Error en la solicitud a AEMET', 
        details: error.response.data
      });
    }
    
    return res.status(500).json({ 
      error: 'Error interno del servidor', 
      message: error.message 
    });
  }
});

// Ruta para servir el HTML principal
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'aemet.html'));
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`Servidor WDC para AEMET corriendo en puerto ${PORT}`);
  console.log(`Abre http://localhost:${PORT} en tu navegador o en Tableau`);
});