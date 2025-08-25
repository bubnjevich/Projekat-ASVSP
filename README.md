# Projekat ASVSP

**Predmet:** Arhitekture sistema velikih skupova podataka  

**Tema:**  
Analiza vremenskih podataka sa fokusom na prepoznavanje obrazaca i trendova u vremenskim događajima širom SAD,  
s ciljem dobijanja korisnih saznanja za različite zainteresovane strane.  

**Opis:**  
Poboljšanje razumevanja vremenskih trendova i njihovih efekata kroz analizu velikih skupova podataka  
i real-time informacija. Projekat koristi distribuiranu arhitekturu zasnovanu na savremenim alatima  
za obradu i vizualizaciju podataka.  

---

## Ciljevi

- Integracija istorijskih i real-time vremenskih podataka  
- Identifikacija sezonskih i regionalnih obrazaca vremenskih događaja  
- Vizualizacija vremenskih trendova i nepogoda radi lakšeg donošenja odluka  
- Podrška različitim zainteresovanim stranama kroz prilagođene izveštaje i dashboard-e  

---

## Korišćene tehnologije

- **Apache Airflow** – orkestracija ETL procesa  
- **Apache Spark** – distribuisana obrada velikih skupova podataka  
- **Kafka** – streaming i real-time prenos podataka  
- **Hive** – skladištenje i upiti nad velikim dataset-ima  
- **Citus/Postgres** – relaciona baza podataka za analitičke upite  
- **Metabase** – vizualizacija podataka i izrada dashboard-a  
- **Hue** – interakcija sa Hadoop i Hive komponentama  

---

## Dataset

Korišćen je javno dostupan skup podataka:  
👉 [US Weather Events (Kaggle)](https://www.kaggle.com/datasets/sobhanmoosavi/us-weather-events)  

---

## Struktura repozitorijuma

