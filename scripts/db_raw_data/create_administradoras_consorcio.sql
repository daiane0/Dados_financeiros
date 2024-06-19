-- Table: public.administradoras_consorcio

-- DROP TABLE IF EXISTS public.administradoras_consorcio;

CREATE TABLE IF NOT EXISTS public.administradoras_consorcio
(
    cnpj character varying COLLATE pg_catalog."default",
    nome_instituicao character varying COLLATE pg_catalog."default",
    endereco character varying COLLATE pg_catalog."default",
    complemento character varying COLLATE pg_catalog."default",
    bairro character varying COLLATE pg_catalog."default",
    cep character varying COLLATE pg_catalog."default",
    municipio character varying COLLATE pg_catalog."default",
    uf character varying COLLATE pg_catalog."default",
    ddd character varying COLLATE pg_catalog."default",
    telefone character varying COLLATE pg_catalog."default",
    email character varying COLLATE pg_catalog."default",
    sitio_internet character varying COLLATE pg_catalog."default",
    municipio_ibge character varying COLLATE pg_catalog."default",
    data timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT administradoras_consorcio_unique UNIQUE (cnpj, nome_instituicao, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, email, sitio_internet, municipio_ibge)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.administradoras_consorcio
    OWNER to postgres;