(function () {
    var myConnector = tableau.makeConnector();
    
    // Definir el esquema según el tipo de datos seleccionado
    myConnector.getSchema = function (schemaCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var dataType = connectionData.dataType;
        var cols = [];
        var tableSchema = {};
        
        if (dataType === "estaciones") {
            cols = [
                { id: "indicativo", dataType: tableau.dataTypeEnum.string },
                { id: "nombre", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", dataType: tableau.dataTypeEnum.string },
                { id: "altitud", dataType: tableau.dataTypeEnum.int },
                { id: "longitud", dataType: tableau.dataTypeEnum.float },
                { id: "latitud", dataType: tableau.dataTypeEnum.float },
                { id: "indsinop", dataType: tableau.dataTypeEnum.string }
            ];
            tableSchema = {
                id: "estacionesAEMET",
                alias: "Estaciones meteorológicas de AEMET",
                columns: cols
            };
        } else if (dataType === "prediccion") {
            cols = [
                { id: "municipio", dataType: tableau.dataTypeEnum.string },
                { id: "provincia", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", dataType: tableau.dataTypeEnum.date },
                { id: "temperatura_maxima", dataType: tableau.dataTypeEnum.float },
                { id: "temperatura_minima", dataType: tableau.dataTypeEnum.float },
                { id: "estado_cielo", dataType: tableau.dataTypeEnum.string },
                { id: "probabilidad_precipitacion", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "prediccionAEMET",
                alias: "Predicción meteorológica diaria AEMET",
                columns: cols
            };
        } else if (dataType === "observacion") {
            cols = [
                { id: "idema", dataType: tableau.dataTypeEnum.string },
                { id: "estacion", dataType: tableau.dataTypeEnum.string },
                { id: "fecha", dataType: tableau.dataTypeEnum.datetime },
                { id: "temperatura", dataType: tableau.dataTypeEnum.float },
                { id: "precipitacion", dataType: tableau.dataTypeEnum.float },
                { id: "humedad_relativa", dataType: tableau.dataTypeEnum.float },
                { id: "velocidad_viento", dataType: tableau.dataTypeEnum.float },
                { id: "direccion_viento", dataType: tableau.dataTypeEnum.float }
            ];
            tableSchema = {
                id: "observacionAEMET",
                alias: "Datos de observación AEMET",
                columns: cols
            };
        }
        
        schemaCallback([tableSchema]);
    };

    // Obtener los datos de la API de AEMET
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var apiKey = connectionData.apiKey;
        var dataType = connectionData.dataType;
        var codigoMunicipio = connectionData.codigoMunicipio || "28079"; // Default a Madrid
        var tableData = [];
        
        // URLs base para diferentes endpoints de la API AEMET
        var baseUrl = "https://opendata.aemet.es/opendata/api";
        var apiUrl = "";
        
        if (dataType === "estaciones") {
            apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
        } else if (dataType === "prediccion") {
            apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
        } else if (dataType === "observacion") {
            apiUrl = baseUrl + "/observacion/convencional/todas";
        }
        
        // Usamos URLs relativas para que funcione tanto en local como en producción
        var proxyUrl = "/proxy/aemet?url=" + encodeURIComponent(apiUrl) + "&apiKey=" + encodeURIComponent(apiKey);
        
        console.log("Conectando a través del proxy:", proxyUrl);
        
        // Primera petición para obtener la URL de los datos
        $.ajax({
            url: proxyUrl,
            type: "GET",
            dataType: "json",
            success: function(resp) {
                console.log("Respuesta inicial:", resp);
                
                if (resp.estado === 200 && resp.datos) {
                    // URL de los datos reales
                    var datosUrl = resp.datos;
                    // Segunda petición a través del proxy
                    var proxyDatosUrl = "/proxy/aemet?url=" + encodeURIComponent(datosUrl) + "&apiKey=" + encodeURIComponent(apiKey);
                    
                    $.getJSON(proxyDatosUrl, function(data) {
                        console.log("Datos obtenidos, número de registros:", Array.isArray(data) ? data.length : "no es array");
                        
                        if (dataType === "estaciones") {
                            // Procesar datos de estaciones
                            for (var i = 0, len = data.length; i < len; i++) {
                                tableData.push({
                                    "indicativo": data[i].indicativo,
                                    "nombre": data[i].nombre,
                                    "provincia": data[i].provincia,
                                    "altitud": parseInt(data[i].altitud) || 0,
                                    "longitud": parseFloat(data[i].longitud) || 0,
                                    "latitud": parseFloat(data[i].latitud) || 0,
                                    "indsinop": data[i].indsinop
                                });
                            }
                        } else if (dataType === "prediccion") {
                            try {
                                // Si data es un array, tomamos el primer elemento
                                if (Array.isArray(data) && data.length > 0) {
                                    var prediccionData = data[0];
                                    
                                    // Verificamos si tiene la estructura esperada
                                    if (prediccionData && prediccionData.prediccion && prediccionData.prediccion.dia) {
                                        var predicciones = prediccionData.prediccion.dia;
                                        for (var i = 0, len = predicciones.length; i < len; i++) {
                                            var dia = predicciones[i];
                                            // Accede con seguridad a las propiedades anidadas
                                            var tempMax = dia.temperatura && dia.temperatura.maxima ? parseFloat(dia.temperatura.maxima) : null;
                                            var tempMin = dia.temperatura && dia.temperatura.minima ? parseFloat(dia.temperatura.minima) : null;
                                            var estadoCielo = dia.estadoCielo && dia.estadoCielo.length > 0 ? dia.estadoCielo[0].descripcion : "";
                                            var probPrecip = dia.probPrecipitacion && dia.probPrecipitacion.length > 0 ? parseFloat(dia.probPrecipitacion[0].value) : 0;
                                            
                                            tableData.push({
                                                "municipio": prediccionData.nombre || "",
                                                "provincia": prediccionData.provincia || "",
                                                "fecha": dia.fecha,
                                                "temperatura_maxima": tempMax,
                                                "temperatura_minima": tempMin,
                                                "estado_cielo": estadoCielo,
                                                "probabilidad_precipitacion": probPrecip
                                            });
                                        }
                                    } else {
                                        console.error("Estructura de datos inesperada dentro del primer elemento:", prediccionData);
                                    }
                                } else {
                                    console.error("Datos de predicción no son un array o está vacío:", data);
                                }
                            } catch (e) {
                                console.error("Error al procesar datos de predicción:", e);
                                console.error("Error detallado:", e.message);
                                console.error("Stack trace:", e.stack);
                            }
                        } else if (dataType === "observacion") {
                            // Procesar datos de observación
                            try {
                                for (var i = 0, len = data.length; i < len; i++) {
                                    var item = data[i];
                                    tableData.push({
                                        "idema": item.idema || "",
                                        "estacion": item.ubi || "",
                                        "fecha": item.fint || "",
                                        "temperatura": parseFloat(item.ta) || null,
                                        "precipitacion": parseFloat(item.prec) || 0,
                                        "humedad_relativa": parseFloat(item.hr) || null,
                                        "velocidad_viento": parseFloat(item.vv) || null,
                                        "direccion_viento": parseFloat(item.dv) || null
                                    });
                                }
                            } catch (e) {
                                console.error("Error al procesar datos de observación:", e);
                                console.error("Error detallado:", e.message);
                                console.error("Stack trace:", e.stack);
                            }
                        }
                        
                        console.log("Filas procesadas:", tableData.length);
                        table.appendRows(tableData);
                        doneCallback();
                    }).fail(function(jqXHR, textStatus, errorThrown) {
                        console.error("Error en segunda petición:");
                        console.error("Status:", textStatus);
                        console.error("Error:", errorThrown);
                        console.error("Response:", jqXHR.responseText);
                        tableau.abortWithError("Error al obtener datos: " + textStatus + " - " + errorThrown);
                    });
                } else {
                    console.error("Error en respuesta inicial:", resp);
                    tableau.abortWithError("Error en la respuesta de la API: " + (resp.estado || "Desconocido") + " - " + (resp.descripcion || ""));
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.error("Error en primera petición:");
                console.error("Status:", textStatus);
                console.error("Error:", errorThrown);
                console.error("Response:", jqXHR.responseText);
                tableau.abortWithError("Error en la conexión a la API de AEMET: " + textStatus + " - " + errorThrown);
            }
        });
    };

    // Inicializar y registrar el conector
    tableau.registerConnector(myConnector);
    
    // Mostrar/ocultar campo de municipio según el tipo de datos
    $(document).ready(function() {
        $('#dataType').change(function() {
            if ($(this).val() === 'prediccion') {
                $('#municipioGroup').show();
            } else {
                $('#municipioGroup').hide();
            }
        });
        
        // Modo de prueba: Si se hace clic en "Probar API" (botón agregado al HTML)
        $("#testButton").click(function() {
            var apiKey = $('#apiKey').val().trim();
            var dataType = $('#dataType').val();
            var codigoMunicipio = $('#codigoMunicipio').val().trim() || "28079";
            
            if (!apiKey) {
                alert("Por favor, introduce una API Key válida de AEMET");
                return;
            }
            
            if (dataType === 'prediccion' && !codigoMunicipio) {
                alert("Para predicciones, debes introducir un código de municipio");
                return;
            }
            
            // Construir URLs
            var baseUrl = "https://opendata.aemet.es/opendata/api";
            var apiUrl = "";
            
            if (dataType === "estaciones") {
                apiUrl = baseUrl + "/valores/climatologicos/inventarioestaciones/todasestaciones";
            } else if (dataType === "prediccion") {
                apiUrl = baseUrl + "/prediccion/especifica/municipio/diaria/" + codigoMunicipio;
            } else if (dataType === "observacion") {
                apiUrl = baseUrl + "/observacion/convencional/todas";
            }
            
            var proxyUrl = "/proxy/aemet?url=" + encodeURIComponent(apiUrl) + "&apiKey=" + encodeURIComponent(apiKey);
            
            // Mostrar spinner o mensaje de carga
            $("#testResults").html("<p>Cargando datos...</p>");
            $("#testResults").show();
            
            // Realizar solicitud
            $.ajax({
                url: proxyUrl,
                type: "GET",
                dataType: "json",
                success: function(resp) {
                    if (resp.estado === 200 && resp.datos) {
                        var datosUrl = resp.datos;
                        var proxyDatosUrl = "/proxy/aemet?url=" + encodeURIComponent(datosUrl) + "&apiKey=" + encodeURIComponent(apiKey);
                        
                        $.getJSON(proxyDatosUrl, function(data) {
                            var resultHTML = "<h4>Prueba exitosa</h4>";
                            
                            if (Array.isArray(data)) {
                                resultHTML += "<p>Se han recibido " + data.length + " registros.</p>";
                                
                                // Mostrar una muestra de los datos
                                resultHTML += "<h5>Muestra de datos:</h5>";
                                resultHTML += "<pre>" + JSON.stringify(data.slice(0, 2), null, 2) + "</pre>";
                            } else {
                                resultHTML += "<p>Datos recibidos correctamente.</p>";
                                resultHTML += "<pre>" + JSON.stringify(data, null, 2).substring(0, 1000) + "...</pre>";
                            }
                            
                            $("#testResults").html(resultHTML);
                        }).fail(function(jqXHR, textStatus, errorThrown) {
                            $("#testResults").html("<h4>Error al obtener datos</h4><p>" + textStatus + " - " + errorThrown + "</p><pre>" + jqXHR.responseText + "</pre>");
                        });
                    } else {
                        $("#testResults").html("<h4>Error en la respuesta inicial</h4><p>Estado: " + (resp.estado || "Desconocido") + "</p><p>Descripción: " + (resp.descripcion || "") + "</p>");
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    $("#testResults").html("<h4>Error en la conexión</h4><p>" + textStatus + " - " + errorThrown + "</p><pre>" + jqXHR.responseText + "</pre>");
                }
            });
        });
        
        // Para Tableau WDC
        $("#submitButton").click(function () {
            var apiKey = $('#apiKey').val().trim();
            var dataType = $('#dataType').val();
            var codigoMunicipio = $('#codigoMunicipio').val().trim();
            
            if (!apiKey) {
                alert("Por favor, introduce una API Key válida de AEMET");
                return;
            }
            
            if (dataType === 'prediccion' && !codigoMunicipio) {
                alert("Para predicciones, debes introducir un código de municipio");
                return;
            }
            
            // Guardar los datos de conexión
            tableau.connectionData = JSON.stringify({
                "apiKey": apiKey,
                "dataType": dataType,
                "codigoMunicipio": codigoMunicipio
            });
            
            // Establecer el nombre de la conexión
            tableau.connectionName = "Datos AEMET - " + dataType;
            
            // Enviar la conexión a Tableau
            tableau.submit();
        });
    });
})();