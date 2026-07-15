- **Data Sharing** permite compartir datasets en vivo entre namespaces/clusters de Redshift **sin duplicar** datos con un ETL de copia.
- El **producer** publica un *datashare* (schemas, tables, views). El **consumer** los consulta como objetos locales.
- Casos típicos: multi-account, multi-team, data mesh — una fuente de verdad, varios consumidores.
- En este workshop no es obligatorio implementar un share cross-account; sí es obligatorio **explicarlo en el reporte**: cuándo lo usarías frente a copiar tablas o exportar a S3.

⸻

## 1. Planificación

- Anotar: URL del dataset, formato (CSV/Parquet), path S3 exacto y 3–5 columnas clave.
- Definir 2–3 **preguntas de negocio** a responder (ej.: totales por categoría, top N barrios, tendencia por mes).
- Elegir nombres únicos:
  - Namespace: `workshop-rs-<tu-nombre>`
  - Workgroup: `workshop-wg-<tu-nombre>`
  - Database: `workshop` (o similar)
- Tags: `owner`, `env=internship`, `workshop=redshift`, `dataset`.
- Modelo: 1–2 tablas (facts + dimension, o una sola tabla si el CSV es flat).

⸻

## 2. IAM: acceso de Redshift a S3

- Crear un **IAM Role** para Redshift Serverless con trust al servicio (`redshift-serverless.amazonaws.com`, según indique la consola al asociar el role).
- Policy mínima:
  - `s3:GetObject` y `s3:ListBucket` sobre el bucket (para `COPY`).
  - `s3:PutObject` sobre el prefijo `analytics/` (para `UNLOAD`).
- Evitar `AmazonS3FullAccess` si se puede acotar al bucket del workshop.
- Guardar el ARN: se asocia al Namespace.

⸻

## 3. Crear Redshift Serverless

- Consola: **Amazon Redshift → Serverless dashboard → Create workgroup**.
- Crear **Namespace** + **Workgroup** con los nombres del paso 1.
- **Base capacity (RPU):** empezar en el mínimo disponible en la región. Documentar el valor elegido y por qué.
- Configurar admin user/password (o IAM auth si el equipo lo indica). **No hardcodear** la password en el reporte.
- Asociar el IAM Role del paso 2 al Namespace.
- Red / acceso:
  - Seguir la guía de la cuenta (VPC, subnets, security group).
  - Habilitar acceso al **Query Editor v2**.
- Tags en Namespace y Workgroup.
- Esperar a que el Workgroup quede **Available** y anotar el endpoint.

⸻

## 4. Conexión y validación

- Abrir **Query Editor v2**, conectar al Workgroup y a la database.
- Ejecutar:

```sql
SELECT current_database(), current_user, version();
```

- Crear un schema de trabajo:

```sql
CREATE SCHEMA IF NOT EXISTS workshop;
SET search_path TO workshop;
```

- Dejar evidencia (captura) de la conexión OK.

⸻

## 5. Modelar tablas (schema-on-write)

- Diseñar el DDL según el dataset. Ejemplo de patrón (adaptar columnas reales):

```sql
CREATE TABLE workshop.hechos_eventos (
  evento_id       VARCHAR(64),
  fecha           DATE,
  barrio          VARCHAR(128),
  categoria       VARCHAR(128),
  monto           DECIMAL(12, 2)
)
DISTSTYLE AUTO
SORTKEY (fecha);
```

- Si hace falta una dimensión (barrios, categorías), crear una segunda tabla con PK y columna de join.
- Documentar data dictionary: field → type → description.
- Usar **tipos explícitos**. No dejar todo como `VARCHAR` si hay fechas y montos.

⸻

## 6. Cargar con COPY

- Desde Query Editor, ejecutar un `COPY` al path S3.
- Ejemplo CSV:

```sql
COPY workshop.hechos_eventos
FROM 's3://<tu-bucket>/raw/<dataset>/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/<tu-role>'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto'
REGION '<tu-region>';
```

- Si el archivo está en **Parquet** (capa curated), usar la sintaxis Parquet del `COPY` y documentar la variante.
- Validar:

```sql
SELECT COUNT(*) AS total_filas FROM workshop.hechos_eventos;
SELECT MIN(fecha), MAX(fecha) FROM workshop.hechos_eventos;
SELECT * FROM workshop.hechos_eventos LIMIT 10;
```

- Si el `COPY` falla, revisar el mensaje (tipos, delimitadores, encoding, IAM, región) y anotar el fix en el reporte.
- Evitar carga fila por fila con `INSERT`.

⸻

## 7. Queries de negocio

- Resolver **al menos 3** preguntas con SQL. Incluir:
  - `WHERE` + agregación (`COUNT` / `SUM` / `AVG`)
  - `GROUP BY` + `ORDER BY` / `LIMIT`
  - Al menos un **JOIN** (si hay una sola tabla flat, crear una dimensión chica o un CTE/subquery agregada y joinearla)
  - Usar `COUNT(*) FILTER (WHERE …)` cuando aplique (mismo criterio que en la clase de RDS)
- Cada query: comentario de 1 línea — qué pregunta de negocio responde.
- Ejemplo (adaptar a la data):

```sql
-- Total y ticket promedio por barrio (top 10)
SELECT
  barrio,
  COUNT(*) AS eventos,
  SUM(monto) AS total_monto,
  AVG(monto) AS ticket_promedio
FROM workshop.hechos_eventos
WHERE fecha >= DATE '2024-01-01'
GROUP BY barrio
ORDER BY total_monto DESC
LIMIT 10;
```

- Incluir queries + resultados (o capturas) en el entregable.

⸻

## 8. Cerrar el loop con el Data Lake (`UNLOAD`)

- Generar un resumen analítico (2–3 métricas + 1–2 dimensiones) con un `SELECT`.
- Exportar a S3 con `UNLOAD` hacia `s3://<tu-bucket>/analytics/redshift/<tu-nombre>/`, preferible en Parquet:

```sql
UNLOAD ('
  SELECT barrio, COUNT(*) AS eventos, SUM(monto) AS total_monto
  FROM workshop.hechos_eventos
  GROUP BY barrio
')
TO 's3://<tu-bucket>/analytics/redshift/<tu-nombre>/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/<tu-role>'
FORMAT AS PARQUET
ALLOWOVERWRITE;
```

- Verificar en S3 que aparecieron objetos; anotar tamaño aproximado + tags (`layer=analytics`, `source=redshift`).
- En el reporte: justificar por qué un resumen vuelve al **Data Lake** (consumo offline, otro pipeline, Athena, etc.).

⸻

## 9. Costos, operación y Data Sharing

En el reporte, responder en forma clara:

- ¿Qué es un RPU y cómo se factura compute vs storage en Serverless?
- ¿Qué diferencia hay entre Namespace y Workgroup en tu setup?
- ¿Por qué Serverless frente a un cluster provisioned para este caso?
- ¿Qué pasa si dejás el Namespace/Workgroup sin borrar?
- ¿Cuándo preferirías Athena vs Redshift para el mismo dataset?
- **Data Sharing:** en un escenario multi-team, ¿cuándo compartirías tablas vía datashare en lugar de duplicar con `COPY`/`UNLOAD`? ¿Qué ventajas y consideraciones de governance mencionarías?

(Opcional) Comparar con RDS: ¿tiene sentido correr estas agregaciones en la base transaccional? ¿Por qué sí o no?

⸻

## 10. Entrega (`REDSHIFT_REPORT.md`)

Un solo archivo en inglés — **`REDSHIFT_REPORT.md`** — con capturas y SQL. Debe incluir al menos:

- **Overview** — qué hace el pipeline en 2–3 oraciones.
- **Architecture** — Namespace / Workgroup / RPU / S3 / IAM / flujo COPY → queries → UNLOAD.
- **Data sources** — URL del dataset, path S3, supuestos (sample vs full).
- **Data dictionary** — tablas Redshift: field → type → description.
- **Load** — comando `COPY` (sin secrets) + conteos de validación.
- **Business questions** — las 3+ queries con resultados.
- **Unload** — path S3 del resumen y evidencia.
- **Data Sharing** — párrafo de diseño: cuándo lo usarías y por qué importa.
- **How to run** — pasos para recrear el entorno (sin passwords en claro).
- **Summary (español)** — qué se hizo, decisiones, supuestos y cómo reproducir.
- **Cost notes** — RPU elegido, recursos a borrar, compute vs storage.

Que alguien que no estuvo en la clase pueda seguirlo solo con ese archivo.

⸻

## 11. Cierre

Deberías poder explicar:

- Diferencia OLTP (RDS) vs OLAP (Redshift).
- Rol del **Data Warehouse** frente al **Data Lake**.
- Namespace vs Workgroup; qué es un RPU y cómo se cobra.
- Por qué columnar ayuda a queries analíticas.
- Qué resuelve `COPY` frente a inserts.
- Cuándo Athena sobre el Data Lake y cuándo Redshift.
- Para qué sirve **Data Sharing**.

Entregar **`REDSHIFT_REPORT.md`** según indique el equipo.

⸻

## ★ Limpieza

- Eliminar el **Workgroup** Serverless.
- Eliminar el **Namespace** asociado.
- Verificar que no queden Workgroups/Namespaces de prueba.
- Si el IAM role era exclusivo del workshop y no se reutiliza, eliminarlo (o acotar policies amplias).
- El bucket S3 del taller puede quedar; limpiar objetos temporales bajo `analytics/redshift/` si el equipo lo pide.
- Revisar Cost Explorer / Billing a los 1–2 días: sin Workgroup no debería haber compute Serverless activo.

⸻

## Troubleshooting

| Síntoma | Mirar primero |
|--------|----------------|
| `COPY` access denied | IAM role mal asociado o sin permisos al bucket/prefijo |
| `COPY` type/parse errors | Delimitador, header, tipos DDL, encoding, fechas |
| No conecta Query Editor | VPC / SG / estado del Workgroup / permisos de consola |
| Primera query lenta | Cold start de Serverless; reintentar antes de subir RPU |
| Facturación inesperada | Workgroup o Namespace sin borrar; storage residual |

No subir base RPU por inercia ante una sola query lenta: diagnosticar primero.