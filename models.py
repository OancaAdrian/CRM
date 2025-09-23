from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Text,
    ForeignKey,
    func,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Firm(Base):
    __tablename__ = "firms"
    __table_args__ = {"schema": "public"}

    cui = Column(String, primary_key=True, index=True)
    denumire = Column(Text)

    cod_inmatriculare = Column(Text)
    data_inmatriculare = Column(Text)
    euid = Column(Text)
    forma_juridica = Column(Text)
    tara = Column(Text)

    judet = Column(Text)
    localitate = Column(Text)
    adr_den_strada = Column(Text)
    adr_nr_strada = Column(Text)
    adr_bloc = Column(Text)
    adr_scara = Column(Text)
    adr_etaj = Column(Text)
    adr_apartament = Column(Text)
    adr_cod_postal = Column(Text)
    adr_sector = Column(Text)
    adr_completare = Column(Text)

    caen = Column(Text)

    stocuri = Column(Text, nullable=True)
    creante = Column(Text, nullable=True)
    datorii = Column(Text, nullable=True)
    provizioane = Column(Text, nullable=True)

    numar_licente = Column(Text, nullable=True)
    telefon = Column(Text)
    manager_de_transport = Column(Text)

    active_imobilizate_total = Column(Text, nullable=True)
    active_circulante_total_din_care = Column(Text, nullable=True)
    casa_si_conturi_la_banci = Column(Text, nullable=True)
    cheltuieli_in_avans = Column(Text, nullable=True)
    venituri_in_avans = Column(Text, nullable=True)
    capitaluri_total_din_care = Column(Text, nullable=True)
    capital_subscris_varsat = Column(Text, nullable=True)
    patrimoniul_regiei = Column(Text, nullable=True)

    cifra_de_afaceri_neta = Column(Text, nullable=True)
    venituri_totale = Column(Text, nullable=True)
    cheltuieli_totale = Column(Text, nullable=True)
    profitul_brut = Column(Text, nullable=True)
    pierdere_bruta = Column(Text, nullable=True)
    profitul_net = Column(Text, nullable=True)
    pierdere_neta = Column(Text, nullable=True)

    numar_mediu_de_salariati = Column(Text, nullable=True)
    an = Column(Text, nullable=True)

    actualizat_la = Column(Text, nullable=True)


class Financial(Base):
    __tablename__ = "financials_annual"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    cui = Column(String, index=True, nullable=False)
    an = Column(Integer, nullable=True)
    cifra_afaceri = Column(Integer, nullable=True)
    profitul_net = Column(Integer, nullable=True)


class ActivityType(Base):
    __tablename__ = "activity_types"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False, unique=True)


class Activity(Base):
    __tablename__ = "activities"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    cui = Column(String, index=True, nullable=False)
    activity_type_id = Column(Integer, ForeignKey("public.activity_types.id"), nullable=True)
    comment = Column(Text, nullable=True)
    score = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    activity_type = relationship("ActivityType", backref="activities")
