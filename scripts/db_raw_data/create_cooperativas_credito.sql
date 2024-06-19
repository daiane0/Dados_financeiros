-- Table: public.cooperativas_credito

-- DROP TABLE IF EXISTS public.cooperativas_credito;

CREATE TABLE IF NOT EXISTS public.cooperativas_credito
(
    cnpj character varying COLLATE pg_catalog."default",
    nome character varying COLLATE pg_catalog."default",
    endereco character varying COLLATE pg_catalog."default",
    complemento character varying COLLATE pg_catalog."default",
    bairro character varying COLLATE pg_catalog."default",
    cep character varying COLLATE pg_catalog."default",
    municipio character varying COLLATE pg_catalog."default",
    uf character varying COLLATE pg_catalog."default",
    ddd character varying COLLATE pg_catalog."default",
    telefone character varying COLLATE pg_catalog."default",
    classe character varying COLLATE pg_catalog."default",
    criterio_associacao character varying COLLATE pg_catalog."default",
    categoria_cooperativa_singular character varying COLLATE pg_catalog."default",
    filiacao character varying COLLATE pg_catalog."default",
    email character varying COLLATE pg_catalog."default",
    sitio_internet character varying COLLATE pg_catalog."default",
    municipio_ibge character varying COLLATE pg_catalog."default",
    data timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT cooperativas_credito_unique UNIQUE (cnpj, nome, endereco, complemento, bairro, cep, municipio, uf, ddd, telefone, classe, criterio_associacao, categoria_cooperativa_singular, filiacao, email, sitio_internet, municipio_ibge)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cooperativas_credito
    OWNER to postgres;