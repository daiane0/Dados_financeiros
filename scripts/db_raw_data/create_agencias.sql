-- Table: public.agencias

-- DROP TABLE IF EXISTS public.agencias;

CREATE TABLE IF NOT EXISTS public.agencias
(
    cnpj character varying COLLATE pg_catalog."default",
    sequencial_cnpj character varying COLLATE pg_catalog."default",
    digitos_verificadores_cnpj character varying COLLATE pg_catalog."default",
    nome_da_instituicao character varying COLLATE pg_catalog."default",
    segmento character varying COLLATE pg_catalog."default",
    cod_comp character varying COLLATE pg_catalog."default",
    nome_agencia character varying COLLATE pg_catalog."default",
    endereco character varying COLLATE pg_catalog."default",
    numero character varying COLLATE pg_catalog."default",
    complemento character varying COLLATE pg_catalog."default",
    bairro character varying COLLATE pg_catalog."default",
    cep character varying COLLATE pg_catalog."default",
    cod_ibge_municipio character varying COLLATE pg_catalog."default",
    municipio character varying COLLATE pg_catalog."default",
    uf character varying COLLATE pg_catalog."default",
    data_inicio character varying COLLATE pg_catalog."default",
    ddd character varying COLLATE pg_catalog."default",
    fone character varying COLLATE pg_catalog."default",
    data character varying COLLATE pg_catalog."default",
    CONSTRAINT agencias_unique UNIQUE (cnpj, sequencial_cnpj, digitos_verificadores_cnpj, nome_da_instituicao, segmento, cod_comp, nome_agencia, endereco, numero, complemento, bairro, cep, cod_ibge_municipio, municipio, uf, data_inicio, ddd, fone)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.agencias
    OWNER to postgres;