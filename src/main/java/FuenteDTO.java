package main.java;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class FuenteDTO {
    private Long regionId;
    private String departamento;
    private String municipio;
    private String decada;
    private String diaVisita;
    private String diaVisitaObligatorio;
    private Long usuarioCodigo;
    private String email;
    private String usuarioNombre;
    private Integer numArticulos;
    private Long fuenteCodigo;
    private String fuenteNombre;
    private String fuenteDireccion;
    private String sector;
    private String fuenteTipo;
    private String fuenteArea;
    private Long ordenEnRuta;
    private BigDecimal latitude;
    private BigDecimal longitude;
    private BigDecimal altitude;
    private Timestamp fechaAlta;
    private Integer anioAlta;
    private Integer mesAlta;
    private String nombreMes;
    private String fuenteEstado;
    private Integer georeferenciada;

   
    public FuenteDTO() {
    }

   
    public Long getRegionId() {
        return regionId;
    }

    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    public String getDepartamento() {
        return departamento;
    }

    public void setDepartamento(String departamento) {
        this.departamento = departamento;
    }

    public String getMunicipio() {
        return municipio;
    }

    public void setMunicipio(String municipio) {
        this.municipio = municipio;
    }

    public String getDecada() {
        return decada;
    }

    public void setDecada(String decada) {
        this.decada = decada;
    }

    public String getDiaVisita() {
        return diaVisita;
    }

    public void setDiaVisita(String diaVisita) {
        this.diaVisita = diaVisita;
    }

    public String getDiaVisitaObligatorio() {
        return diaVisitaObligatorio;
    }

    public void setDiaVisitaObligatorio(String diaVisitaObligatorio) {
        this.diaVisitaObligatorio = diaVisitaObligatorio;
    }

    public Long getUsuarioCodigo() {
        return usuarioCodigo;
    }

    public void setUsuarioCodigo(Long usuarioCodigo) {
        this.usuarioCodigo = usuarioCodigo;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUsuarioNombre() {
        return usuarioNombre;
    }

    public void setUsuarioNombre(String usuarioNombre) {
        this.usuarioNombre = usuarioNombre;
    }

    public Integer getNumArticulos() {
        return numArticulos;
    }

    public void setNumArticulos(Integer numArticulos) {
        this.numArticulos = numArticulos;
    }

    public Long getFuenteCodigo() {
        return fuenteCodigo;
    }

    public void setFuenteCodigo(Long fuenteCodigo) {
        this.fuenteCodigo = fuenteCodigo;
    }

    public String getFuenteNombre() {
        return fuenteNombre;
    }

    public void setFuenteNombre(String fuenteNombre) {
        this.fuenteNombre = fuenteNombre;
    }

    public String getFuenteDireccion() {
        return fuenteDireccion;
    }

    public void setFuenteDireccion(String fuenteDireccion) {
        this.fuenteDireccion = fuenteDireccion;
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public String getFuenteTipo() {
        return fuenteTipo;
    }

    public void setFuenteTipo(String fuenteTipo) {
        this.fuenteTipo = fuenteTipo;
    }

    public String getFuenteArea() {
        return fuenteArea;
    }

    public void setFuenteArea(String fuenteArea) {
        this.fuenteArea = fuenteArea;
    }

    public Long getOrdenEnRuta() {
        return ordenEnRuta;
    }

    public void setOrdenEnRuta(Long ordenEnRuta) {
        this.ordenEnRuta = ordenEnRuta;
    }

    public BigDecimal getLatitude() {
        return latitude;
    }

    public void setLatitude(BigDecimal latitude) {
        this.latitude = latitude;
    }

    public BigDecimal getLongitude() {
        return longitude;
    }

    public void setLongitude(BigDecimal longitude) {
        this.longitude = longitude;
    }

    public BigDecimal getAltitude() {
        return altitude;
    }

    public void setAltitude(BigDecimal altitude) {
        this.altitude = altitude;
    }

    public Timestamp getFechaAlta() {
        return fechaAlta;
    }

    public void setFechaAlta(Timestamp fechaAlta) {
        this.fechaAlta = fechaAlta;
    }

    public Integer getAnioAlta() {
        return anioAlta;
    }

    public void setAnioAlta(Integer anioAlta) {
        this.anioAlta = anioAlta;
    }

    public Integer getMesAlta() {
        return mesAlta;
    }

    public void setMesAlta(Integer mesAlta) {
        this.mesAlta = mesAlta;
    }

    public String getNombreMes() {
        return nombreMes;
    }

    public void setNombreMes(String nombreMes) {
        this.nombreMes = nombreMes;
    }

    public String getFuenteEstado() {
        return fuenteEstado;
    }

    public void setFuenteEstado(String fuenteEstado) {
        this.fuenteEstado = fuenteEstado;
    }

    public Integer getGeoreferenciada() {
        return georeferenciada;
    }

    public void setGeoreferenciada(Integer georeferenciada) {
        this.georeferenciada = georeferenciada;
    }
}