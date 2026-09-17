# Cahier des charges — `io-scheduler`

## 1. Objectif

`io-scheduler` est une bibliothèque Rust de scheduling asynchrone de flux segmentés.

Elle manipule :

* des trames opaques `T` ;
* des fichiers opaques ;
* une séquence logique ordonnée de fichiers ;
* une capacité globale de buffering exprimée en nombre de trames ;
* une ouverture asynchrone des fichiers ;
* plusieurs opérations I/O pouvant progresser concurremment ;
* une sortie logique strictement ordonnée.

La bibliothèque ne connaît pas :

* la taille en octets d'une trame ;
* la représentation physique d'un fichier ;
* les offsets internes d'un fichier ;
* les ranges ou sous-plages ;
* le protocole utilisé ;
* la manière dont un fichier est produit ou consommé ;
* le calcul métier de la fenêtre de buffering ;
* le modèle de débit ou de latence ;
* la structure métier des métadonnées de fichier.

La bibliothèque raisonne exclusivement en :

```text
ordre
nombre de trames
buffering
ouverture anticipée
concurrence
backpressure
finalisation
retry
```

Les priorités sont, dans cet ordre :

```text
1. Performance
2. Simplicité du hot path
3. Robustesse
4. Simplicité de l'API
5. Généralité
```

Toute abstraction qui augmente significativement la complexité ou le coût du hot path doit être évitée si elle n'est pas strictement nécessaire.

---

# 2. Modèle mental général

`io-scheduler` est constitué de deux schedulers symétriques.

## Lecture

```text
Stream<ReadFile>
        │
        ▼
   ReadScheduler<T>
        │
        ▼
     Stream<T>
```

Les fichiers sont fournis dans leur ordre logique.

Le scheduler :

1. découvre les fichiers ;
2. peut appeler `open()` sur plusieurs fichiers en avance ;
3. conserve les readers ouverts ;
4. autorise seulement un préfixe logique borné de trames à entrer dans les buffers ;
5. poll plusieurs readers concurremment ;
6. réordonne implicitement les résultats par fichier ;
7. émet les `T` dans l'ordre logique global.

---

## Écriture

```text
Stream<T> + Stream<WriteFile>
             │
             ▼
       WriteScheduler<T>
             │
             ▼
          Stream<R>
```

Les trames arrivent dans leur ordre logique.

Les fichiers destinations sont également fournis dans leur ordre logique.

Le scheduler :

1. découvre les fichiers d'écriture ;
2. peut les ouvrir en avance ;
3. affecte les trames aux fichiers selon leur capacité ;
4. bufferise les trames dans le scheduler ;
5. fait progresser plusieurs writers concurremment ;
6. conserve les trames d'un fichier jusqu'à sa finalisation ;
7. rejoue un fichier en cas d'échec ;
8. retourne les résultats de finalisation `R` dans l'ordre logique.

---

# 3. Trame `T`

Une trame est l'unité fondamentale de scheduling.

`T` est opaque.

Exemples conceptuels :

```rust
T
```

peut représenter n'importe quel objet `Sized`.

La bibliothèque n'a pas besoin de connaître :

```text
taille mémoire de T
taille sérialisée de T
contenu de T
structure interne de T
```

Une fenêtre de :

```text
64 frames
```

signifie exactement :

> au maximum 64 unités logiques de scheduling peuvent être engagées conformément à la sémantique du scheduler.

Elle ne représente aucune quantité d'octets.

---

# 4. Principe fondamental : les fichiers sont opaques

`io-scheduler` ne connaît jamais la structure interne d'un fichier.

Il ne doit exister aucune API :

```rust
open(range)
open(offset)
seek(...)
read_from(...)
```

dans la bibliothèque.

Le contrat est simplement :

```rust
open()
```

Un fichier fourni au scheduler représente déjà exactement le contenu logique qui doit être lu ou écrit.

Si une couche supérieure souhaite représenter une vue partielle ou transformée d'un fichier, cette vue doit être encapsulée dans l'implémentation du fichier avant d'être donnée à `io-scheduler`.

Pour le scheduler :

```text
File
=
suite logique complète de N trames
```

---

# 5. API de lecture

API conceptuelle :

```rust
pub trait ReadFile<T> {
    type Error;

    type Open: Future<
        Output = Result<Self::Reader, Self::Error>
    > + Unpin;

    type Reader: Stream<
        Item = Result<T, Self::Error>
    > + Unpin;

    fn frame_count(&self) -> u32;

    fn open(&self) -> Self::Open;
}
```

## Contrat

`frame_count()` retourne le nombre exact de trames que le reader doit produire.

Exemple :

```rust
file.frame_count() == 6
```

signifie que le reader issu de :

```rust
file.open()
```

doit produire exactement 6 `T`, puis EOF.

Le scheduler ne manipule jamais d'index local servant à rouvrir une portion du fichier.

---

# 6. Ouverture et lecture sont deux choses différentes

C'est un invariant central du design.

Un fichier peut être :

```text
découvert
↓
open() lancé
↓
open() terminé
↓
Reader disponible
```

sans avoir encore le droit de produire la moindre trame dans le buffer.

Autrement dit :

> `open()` peut avoir lieu très longtemps avant le premier `poll_next()` utile sur le reader.

Le scheduler maintient donc deux horizons distincts :

```text
OPEN FRONTIER
BUFFER FRONTIER
```

---

# 7. Open frontier

La frontière d'ouverture contrôle jusqu'où le scheduler peut préparer les fichiers futurs.

Exemple :

```text
F0    F1    F2    F3    F4
│     │     │     │
open  open  open  opening
```

Même si seul `F0` est actuellement autorisé à fournir des trames.

L'ouverture anticipée permet de masquer la latence de `open()`.

Elle ne consomme pas de capacité de trames.

---

# 8. Buffer frontier en lecture

La fenêtre de buffering est un préfixe contigu du flux logique restant.

Exemple :

```text
F0.frame_count = 6
F1.frame_count = 5
F2.frame_count = 10
```

Si :

```text
buffered_frames = 4
```

la distribution logique est :

```text
F0 = 4
F1 = 0
F2 = 0
```

Si :

```text
buffered_frames = 8
```

alors :

```text
F0 = 6
F1 = 2
F2 = 0
```

Si :

```text
buffered_frames = 13
```

alors :

```text
F0 = 6
F1 = 5
F2 = 2
```

Le scheduler ne distribue jamais arbitrairement la capacité aux fichiers futurs.

La fenêtre est toujours :

> les N prochaines trames logiques du flux.

---

# 9. Concurrence à l'intérieur de la fenêtre read

Le fait que la fenêtre soit logique et ordonnée n'interdit pas la concurrence.

Exemple :

```text
buffered_frames = 8

F0 quota logique = 6
F1 quota logique = 2
```

Le scheduler peut poller simultanément :

```text
Reader F0
Reader F1
```

jusqu'à leurs quotas respectifs.

Il est possible que :

```text
F0 ait produit 1/6
F1 ait produit 2/2
```

Le scheduler conserve alors les deux trames de F1 en attente.

Elles ne peuvent pas être émises avant la fin logique des trames précédentes de F0.

---

# 10. Ordre en lecture

L'ordre des fichiers entrants définit l'ordre global.

Si :

```text
F0 = A B C
F1 = D E
F2 = F G
```

la sortie doit être :

```text
A B C D E F G
```

indépendamment de l'ordre réel d'arrivée dans les buffers.

Les readers peuvent progresser dans n'importe quel ordre physique.

La sortie reste strictement ordonnée.

---

# 11. État minimal d'un slot de lecture

Structure conceptuelle :

```rust
struct ReadSlot<F, O, R, T> {
    file: F,

    frame_count: u32,

    received: u32,
    emitted: u32,
    allowed: u32,

    buffer: VecDeque<T>,

    io: ReadIo<O, R>,
}
```

Avec :

```rust
enum ReadIo<O, R> {
    Opening(O),
    Ready(R),
    Done,
}
```

`Ready` signifie uniquement :

> le reader existe.

Cela ne signifie pas :

> il doit être pollé maintenant.

Le scheduler le poll uniquement si :

```rust
received < allowed
```

---

# 12. Pas d'état `Reading`

Aucun état explicite :

```rust
Reading
Blocked
Waiting
Authorized
```

n'est nécessaire.

L'activité du reader est dérivée directement des compteurs :

```rust
received < allowed
```

La règle générale du projet est :

> ne jamais stocker un état qui peut être obtenu trivialement à partir des compteurs existants.

---

# 13. Fenêtre glissante de lecture

Lorsque le consumer consomme une trame, la fenêtre avance d'une trame.

Exemple :

```text
buffered_frames = 8

avant :

F0 remaining = 6
F1 allowed   = 2
```

Après émission d'une trame de F0 :

```text
F0 remaining = 5
F1 allowed   = 3
```

La fenêtre conserve toujours une largeur logique maximale de 8 trames, sous réserve des capacités disponibles globalement.

---

# 14. API d'écriture

API conceptuelle :

```rust
pub trait WriteFile<T> {
    type Error;
    type Output;

    type Open: Future<
        Output = Result<Self::Writer, Self::Error>
    > + Unpin;

    type Writer: FrameWriter<
        T,
        Error = Self::Error,
        Output = Self::Output,
    > + Unpin;

    fn frame_capacity(&self) -> u32;

    fn open(&self) -> Self::Open;
}
```

`frame_capacity()` définit le nombre maximal de trames pouvant appartenir à ce fichier.

---

# 15. API du writer

```rust
pub trait FrameWriter<T>: Unpin {
    type Error;
    type Output;

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>>;

    fn poll_finalize(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>>;
}
```

Le writer ne prend jamais possession de `T`.

C'est volontaire.

Le scheduler doit pouvoir conserver toutes les trames d'un fichier jusqu'à sa finalisation.

---

# 16. Pourquoi `poll_write(&T)`

Avec :

```rust
poll_write(&T)
```

le scheduler reste propriétaire de la trame.

Il peut donc :

```text
write A
write B
write C
finalize échoue
↓
reopen
↓
write A
write B
write C
```

sans :

```text
Clone<T>
Arc<T>
copie secondaire
journal externe
```

Le buffer du scheduler est lui-même le journal de replay.

---

# 17. Segmentation en écriture

Supposons :

```text
capacity(F0) = 4
capacity(F1) = 4
```

et :

```text
input = A B C D E F
```

Alors :

```text
F0 = A B C D
F1 = E F
```

La règle fondamentale est :

> la prochaine trame passe au fichier suivant uniquement lorsque le fichier courant a reçu `frame_capacity()` trames.

Aucun état `Sealed` n'est nécessaire.

---

# 18. Pas d'état `Sealed`

La frontière est dérivable :

```rust
frames.len() == capacity
```

Donc aucun :

```rust
sealed: bool
```

ou :

```rust
State::Sealed
```

ne doit être stocké.

Pour le dernier fichier, EOF de l'input signifie que sa frontière est connue même s'il est partiellement rempli.

---

# 19. Écriture immédiate

Un writer peut commencer à écrire dès la première trame.

Il n'est jamais nécessaire d'attendre que le fichier soit rempli.

Exemple :

```text
A arrive
↓
buffer A
↓
Writer0 écrit A

B arrive
↓
buffer B
↓
Writer0 écrit B
```

Le buffering et l'écriture progressent en parallèle.

---

# 20. Concurrence des writers

Plusieurs writers peuvent écrire simultanément.

Exemple :

```text
capacity = 4

F0 = A B C D
F1 = E F G H
F2 = I J
```

État valide :

```text
F0 : finalizing
F1 : writing
F2 : writing
F3 : opening
```

Le writer de F1 peut commencer dès que E est affecté à F1.

Cela implique seulement que :

```text
F0 a déjà reçu ses 4 trames
```

Il n'est pas nécessaire que F0 soit :

```text
entièrement écrit
finalisé
committé
```

---

# 21. Ouverture anticipée en écriture

Comme pour la lecture :

```text
open()
```

est indépendant du début réel de l'I/O de trames.

Un fichier futur peut avoir son writer entièrement prêt avant sa première trame.

Exemple :

```text
F0 Writing
F1 Writer Ready
F2 Opening
F3 Known
```

Quand la première trame de F1 arrive, l'écriture peut commencer immédiatement.

---

# 22. Buffer d'écriture

Chaque slot d'écriture possède un :

```rust
Vec<T>
```

Structure conceptuelle :

```rust
struct WriteSlot<F, O, W, T, R> {
    file: F,

    capacity: u32,

    frames: Vec<T>,
    written: usize,

    io: WriteIo<O, W, R>,

    retries: u32,
}
```

`frames` contient toutes les trames appartenant au fichier.

`written` indique combien ont été acceptées par la tentative actuelle.

---

# 23. État I/O minimal d'écriture

```rust
enum WriteIo<O, W, R> {
    Opening(O),
    Ready(W),
    Done(R),
}
```

Aucun état :

```text
Collecting
Sealed
Writing
Finishing
Committed
Retrying
Blocked
```

n'est nécessaire.

Les actions sont calculées à partir de :

```text
frames.len()
capacity
written
input_eof
WriteIo
retries
```

---

# 24. Détermination de l'action writer

Si :

```rust
written < frames.len()
```

alors :

```rust
poll_write(&frames[written])
```

Si :

```rust
written == frames.len()
```

et que le fichier peut encore recevoir des trames :

```text
aucune action writer supplémentaire
```

Si :

```rust
written == frames.len()
```

et que la frontière du fichier est connue :

```rust
poll_finalize()
```

---

# 25. Quand un fichier est complet

Un fichier est complet si :

```rust
frames.len() == capacity
```

ou, pour le dernier fichier :

```text
input EOF
+
frames non vide
```

Aucun champ supplémentaire n'est nécessaire.

---

# 26. Résultat de finalisation

`poll_finalize()` retourne :

```rust
Output = R
```

`R` est opaque.

Le scheduler ne connaît pas sa signification.

Il sait uniquement :

> ce fichier est définitivement finalisé et son résultat logique est `R`.

---

# 27. Sortie du WriteScheduler

Le `WriteScheduler` est :

```text
Stream<R>
```

conceptuellement :

```text
Stream<T>
+
Stream<WriteFile>
↓
WriteScheduler
↓
Stream<R>
```

L'ordre de sortie des `R` est identique à l'ordre des fichiers fournis.

---

# 28. Finalisations hors ordre

Les fichiers peuvent se finaliser dans n'importe quel ordre.

Exemple :

```text
F0 : Writing
F1 : Done(R1)
F2 : Done(R2)
```

Le scheduler conserve :

```text
R1
R2
```

mais ne les émet pas.

Quand F0 devient :

```text
Done(R0)
```

le scheduler peut retourner :

```text
R0
R1
R2
```

dans cet ordre.

---

# 29. Libération du buffer après finalize

Une fois :

```rust
poll_finalize() -> Ready(Ok(R))
```

les trames du fichier peuvent être libérées immédiatement.

Il n'est pas nécessaire d'attendre que `R` soit émis.

Exemple :

```text
F1 finalisé
R1 bloqué derrière R0

frames F1 :
peuvent déjà être libérées
```

Le scheduler conserve uniquement `R1`.

---

# 30. Retry d'écriture

La politique est volontairement fixe.

Il n'existe pas de :

```text
RecoveryPolicy
RetryPolicy
ErrorClassifier
```

Le comportement est :

```text
erreur open/write/finalize
↓
si retry disponible
    abandonner writer courant
    written = 0
    reopen()
    rejouer frames depuis 0
sinon
    erreur globale
```

Configuration :

```rust
max_retries: u32
```

---

# 31. Rétention obligatoire en écriture

Une trame reste détenue par le scheduler jusqu'au succès du `finalize()` correspondant.

Invariant :

```text
write réussi
≠
trame libérable
```

Seul :

```text
finalize réussi
```

autorise la libération.

---

# 32. Retry lecture

Le scheduler ne connaît aucun moyen générique de reprendre un fichier à une position donnée.

Il ne possède aucune notion de range ou d'offset.

Par conséquent :

* une erreur pendant `open()` peut être retryée ;
* une erreur provenant du reader après ouverture est remontée comme erreur globale.

Si une implémentation souhaite offrir une lecture tolérante aux erreurs internes, elle doit fournir un `Reader` qui implémente lui-même cette résilience.

Cela maintient la frontière d'abstraction propre :

```text
io-scheduler
ne connaît jamais
comment reprendre un File
```

---

# 33. EOF prématuré en lecture

Si :

```rust
frame_count() == 10
```

mais que le reader retourne EOF après seulement 8 trames :

```text
erreur de contrat
```

Le scheduler doit considérer cela comme fatal.

Même chose si le reader retourne plus de 10 trames.

---

# 34. Contrat exact d'un ReadFile

Pour chaque appel réussi à :

```rust
open()
```

le `Reader` doit produire exactement :

```rust
frame_count()
```

trames.

Ni plus.

Ni moins.

---

# 35. `FrameBudget`

La bibliothèque fournit un budget global partagé :

```rust
pub struct FrameBudget {
    ...
}
```

Il est exprimé en trames.

Exemple :

```rust
FrameBudget::new(4096)
```

signifie :

> tous les schedulers partageant ce budget ne peuvent pas engager plus de 4096 trames selon les règles du budget.

---

# 36. Budget global partagé

Exemple :

```text
FrameBudget(4096)

├── ReadScheduler A
├── ReadScheduler B
├── WriteScheduler C
└── WriteScheduler D
```

Les schedulers peuvent obtenir des capacités différentes selon la disponibilité.

---

# 37. `FramePermit`

Chaque scheduler possède un permit représentant sa capacité actuellement accordée.

```rust
pub struct FramePermit {
    ...
}
```

Exemple :

```text
permit = 128
frames réellement présentes = 47
```

Le scheduler peut encore engager jusqu'à 81 trames sans consulter le budget global.

---

# 38. Aucun lock par trame

Le budget doit être manipulé par blocs.

Il est interdit que le hot path fasse :

```text
acquire global
release global
```

pour chaque frame.

Le scheduler acquiert une capacité :

```text
N frames
```

puis travaille localement à l'intérieur de cette capacité.

---

# 39. Fenêtre locale

Configuration conceptuelle :

```rust
pub struct Window {
    pub target_frames: usize,
    pub open_ahead_frames: usize,
    pub max_active_files: usize,
}
```

---

# 40. `target_frames`

`target_frames` représente la quantité souhaitée de trames dans la fenêtre logique active.

Le budget global peut accorder moins.

Exemple :

```text
target_frames = 128
granted_frames = 73
```

La fenêtre effective est alors limitée à 73.

---

# 41. `open_ahead_frames`

`open_ahead_frames` détermine jusqu'où dans le flux logique les fichiers doivent être découverts et préparés.

Cette capacité :

```text
ne compte pas des T
```

Elle sert uniquement à anticiper les `open()`.

---

# 42. `max_active_files`

Cette limite borne le nombre de fichiers simultanément dans un état actif :

```text
Opening
ou
Reader/Writer Ready
```

Elle empêche l'ouverture anticipée de créer un nombre arbitraire de ressources actives.

---

# 43. Buffering et open-ahead sont orthogonaux

Exemple :

```text
target_frames = 8
open_ahead_frames = 50
```

Il est parfaitement valide d'avoir :

```text
F0 : 6 frames autorisées
F1 : 2 frames autorisées
F2 : 0
F3 : 0
F4 : 0
```

tout en ayant :

```text
F0 Reader Ready
F1 Reader Ready
F2 Reader Ready
F3 Opening
F4 Opening
```

L'ouverture en avance ne donne aucune autorisation de produire des trames.

---

# 44. Dimensionnement dynamique

Le calcul de `Window` ne fait pas partie de la bibliothèque.

La couche supérieure peut librement calculer :

```text
target_frames
open_ahead_frames
max_active_files
```

selon ses propres critères.

`io-scheduler` reçoit uniquement le résultat.

API possible :

```rust
scheduler.set_window(window);
```

La bibliothèque ne connaît pas l'origine de cette valeur.

---

# 45. Pas de runtime imposé

Le cœur utilise uniquement :

```text
Future
Stream
Context
Poll
Waker
```

Il ne dépend pas d'un mécanisme d'exécution particulier.

---

# 46. Pas de tâches internes

Chaque scheduler est une seule machine asynchrone.

Pas de :

```text
spawn
channel interne
worker task
thread interne
```

Le scheduler poll lui-même tous ses composants.

---

# 47. Modèle d'exécution

Conceptuellement :

```text
executor
   ↓
scheduler.poll_next()
   ├── découvrir fichiers
   ├── poll open()
   ├── ajuster fenêtre
   ├── poll readers/writers autorisés
   ├── bufferiser
   ├── finaliser
   └── produire sortie
```

---

# 48. Static dispatch

L'API centrale doit privilégier :

```text
associated types
generics
monomorphisation
```

Elle ne doit pas imposer :

```text
Box<dyn ...>
Arc<dyn ...>
BoxFuture
```

L'utilisateur reste libre d'utiliser du dynamic dispatch s'il le souhaite.

---

# 49. `Unpin`

Pour garder l'implémentation simple et rapide, la V1 peut imposer :

```text
Open: Unpin
Reader: Unpin
Writer: Unpin
Streams d'entrée: Unpin
```

Un utilisateur ayant un objet `!Unpin` peut effectuer le boxing/pinning à la frontière de son implémentation.

---

# 50. Structures internes read

Structure recommandée :

```rust
VecDeque<ReadSlot<...>>
```

Le front correspond toujours au prochain fichier logique dont des trames peuvent être émises.

Les fichiers suivants peuvent être ouverts et lus dans la limite de leur quota logique.

---

# 51. Structures internes write

Structure recommandée :

```rust
VecDeque<WriteSlot<...>>
```

Le front correspond au prochain résultat `R` devant être retourné.

Les slots suivants peuvent progresser, écrire et même finaliser avant lui.

---

# 52. Cursor de remplissage write

Comme plusieurs fichiers futurs peuvent déjà être ouverts, il faut connaître quel fichier reçoit actuellement les nouvelles trames.

Un simple curseur suffit :

```rust
fill_index: usize
```

Quand :

```rust
slots[fill_index].frames.len()
    == slots[fill_index].capacity
```

alors :

```rust
fill_index += 1;
```

---

# 53. Cursor de polling

Pour éviter qu'un slot toujours prêt monopolise le scheduler :

```rust
poll_cursor: usize
```

peut servir à scanner les slots actifs en round-robin.

Exemple :

```text
scan 1 :
0 1 2 3

scan 2 :
1 2 3 0

scan 3 :
2 3 0 1
```

---

# 54. Work budget par invocation de `poll`

Une source purement synchrone ou toujours prête peut permettre un nombre infini d'opérations lors d'un seul `poll_next()`.

Le scheduler doit donc utiliser une limite interne de travail.

Exemple :

```rust
const MAX_POLL_OPS: usize = ...;
```

Après épuisement :

```rust
cx.waker().wake_by_ref();
return Poll::Pending;
```

Cette limite reste interne.

Elle ne doit pas compliquer l'API publique.

---

# 55. Règle des wakers

Le scheduler ne doit pas appeler :

```rust
wake_by_ref()
```

simplement parce qu'il retourne `Pending`.

Il s'auto-réveille uniquement s'il sait qu'il reste immédiatement du travail mais interrompt volontairement son exécution pour respecter son work budget.

Sinon, il dépend des wakers des futures et streams sous-jacents.

---

# 56. Erreurs

API conceptuelle :

```rust
pub enum SchedulerError<E> {
    Backend(E),
    Contract(ContractError),
}
```

`Backend(E)` représente les erreurs des fichiers, readers, writers ou streams fournis.

`ContractError` représente uniquement les violations du contrat de `io-scheduler`.

---

# 57. Erreurs de contrat possibles

Exemples :

```rust
pub enum ContractError {
    ZeroFrameCount,
    ZeroFrameCapacity,

    UnexpectedEof,
    TooManyFrames,

    MissingWriteFile,

    FrameCapacityExceedsBudget,
}
```

La liste doit rester courte.

---

# 58. EOF en écriture

À EOF du `Stream<T>` :

```rust
input_eof = true;
```

Trois cas.

## Aucun T n'a été produit

Aucun fichier n'est finalisé.

Les fichiers ouverts en avance sont simplement abandonnés.

## Fichier courant partiel

Exemple :

```text
capacity = 4

A B
EOF
```

Le fichier est finalisé avec 2 trames.

## Fichier exactement rempli

Le fichier est finalisé normalement.

Aucun fichier vide supplémentaire n'est créé.

---

# 59. Insuffisance de fichiers write

Si des trames existent encore mais que le stream de `WriteFile` est terminé :

```text
erreur fatale
```

Exemple :

```text
input :
A B C D E

files :
F0 capacity 4
EOF
```

`E` ne peut être affecté à aucun fichier.

Le scheduler retourne :

```text
MissingWriteFile
```

---

# 60. Capacité write et budget

Comme les trames d'un fichier doivent être conservées jusqu'au `finalize`, le budget global doit pouvoir contenir au minimum un fichier entier.

Si :

```text
file.frame_capacity() > FrameBudget.total_capacity()
```

le scheduler doit échouer immédiatement.

Sinon il serait possible d'obtenir un deadlock structurel :

```text
budget saturé
↓
fichier pas encore complet
↓
impossible de finalize
↓
impossible de libérer le budget
```

---

# 61. `target_frames` n'est pas une hard limit write

Si :

```text
target_frames = 32
```

mais que le fichier courant nécessite :

```text
100 frames
```

le scheduler doit pouvoir temporairement dépasser la cible locale jusqu'à 100, dans la limite du budget global.

La cible est un objectif de fonctionnement.

Le budget global est la contrainte réelle.

---

# 62. Libération du budget

## Read

Une frame cesse d'occuper le scheduler lorsqu'elle est remise au consumer.

## Write

Une frame cesse d'occuper le scheduler uniquement lorsque le fichier auquel elle appartient est finalisé avec succès.

---

# 63. Cancellation

`drop(scheduler)` doit suffire à annuler tout le travail.

Comme aucune tâche détachée n'est créée :

```text
drop Scheduler
↓
drop open futures
drop readers/writers
drop buffers
drop permits
```

Aucune activité propre à `io-scheduler` ne doit survivre.

---

# 64. Finalisation write réussie avant erreur globale

Il est possible qu'un fichier futur ait déjà été finalisé avant qu'un fichier précédent échoue définitivement.

Exemple :

```text
F0 : échec définitif
F1 : Done(R1)
F2 : Done(R2)
```

Le scheduler échoue globalement.

`io-scheduler` ne tente pas d'annuler ou de défaire les finalisations déjà réussies.

La notion de rollback global ne fait pas partie de son contrat.

---

# 65. Buffers recommandés

## Read

```rust
VecDeque<T>
```

Motif :

```text
push_back
pop_front
```

## Write

```rust
Vec<T>
```

Motif :

```text
append séquentiel
replay séquentiel
aucun retrait avant finalize
```

---

# 66. Complexité visée

Pour une trame normale :

```text
O(1) amorti
```

Il ne doit pas y avoir :

```text
HashMap
BTree
recherche globale
tri
allocation scheduler par frame
```

sur le hot path.

---

# 67. Objectif performance

Le scheduler doit viser :

```text
aucun spawn par fichier
aucun channel interne
aucun lock par frame
aucun atomic global par frame
aucun Clone<T> imposé
aucun Arc<T> imposé
aucune allocation heap par frame imposée
aucun dynamic dispatch imposé
```

La concurrence provient uniquement du polling de plusieurs objets asynchrones depuis une même machine d'état.

---

# 68. Algorithme conceptuel ReadScheduler

```rust
loop {
    progressed = false;

    progressed |= poll_file_stream();
    progressed |= poll_open_ahead();

    progressed |= adjust_frame_window();

    progressed |= poll_allowed_readers();

    if front_has_frame() {
        return Ready(Some(Ok(pop_front_frame())));
    }

    discard_completed_front_files();

    if fully_finished() {
        return Ready(None);
    }

    if work_budget_exhausted() {
        wake_self();
        return Pending;
    }

    if !progressed {
        return Pending;
    }
}
```

---

# 69. Algorithme conceptuel WriteScheduler

```rust
loop {
    progressed = false;

    progressed |= poll_file_stream();
    progressed |= poll_open_ahead();

    progressed |= adjust_frame_budget();

    progressed |= poll_input();

    progressed |= poll_active_writers();

    if front_result_ready() {
        return Ready(Some(Ok(take_front_result())));
    }

    discard_unused_open_files_after_eof();

    if everything_finished() {
        return Ready(None);
    }

    if fatal_error {
        return Ready(Some(Err(error)));
    }

    if work_budget_exhausted() {
        wake_self();
        return Pending;
    }

    if !progressed {
        return Pending;
    }
}
```

---

# 70. Invariants Read

Les invariants suivants doivent toujours être vrais.

1. Les fichiers sont interprétés dans l'ordre du stream d'entrée.
2. La fenêtre de buffering est un préfixe contigu du flux logique restant.
3. Un reader n'est jamais pollé au-delà de son quota logique courant.
4. Un reader peut être ouvert longtemps avant de recevoir un quota non nul.
5. Plusieurs readers autorisés peuvent progresser simultanément.
6. Les `T` sont toujours émis dans l'ordre logique.
7. Un fichier produit exactement `frame_count()` trames.
8. Le buffering total ne dépasse jamais la capacité accordée.
9. Les fichiers ouverts en avance ne consomment pas de budget de trames.
10. EOF prématuré ou surplus de trames est fatal.

---

# 71. Invariants Write

1. Les `T` sont affectés aux fichiers dans l'ordre.
2. Un fichier reçoit au maximum `frame_capacity()` trames.
3. La première trame d'un fichier N+1 n'est affectée qu'après remplissage de N.
4. Un writer peut commencer dès la première trame.
5. Plusieurs writers peuvent progresser simultanément.
6. Toutes les trames restent détenues jusqu'au succès du `finalize`.
7. Un retry rejoue toujours le fichier depuis sa première trame.
8. Les résultats `R` sont émis dans l'ordre des fichiers.
9. Un résultat finalisé peut libérer ses trames même s'il ne peut pas encore être émis.
10. Aucun fichier vide final n'est créé.
11. Un dernier fichier partiel est autorisé.
12. La somme des ressources de trames engagées respecte toujours le budget global.

---

# 72. Tests Read indispensables

```text
read_preserves_global_order

read_opens_files_before_buffer_window_reaches_them

read_does_not_poll_ready_reader_outside_buffer_window

read_window_is_contiguous_across_files

read_window_moves_when_front_frames_are_consumed

read_allows_multiple_readers_inside_window

read_buffers_future_file_while_front_file_is_slow

read_respects_global_frame_budget

read_rejects_early_eof

read_rejects_extra_frames

read_propagates_reader_error

read_releases_budget_on_drop

read_drops_prefetched_unused_files
```

---

# 73. Tests Write indispensables

```text
write_starts_writer_on_first_frame

write_opens_future_files_before_they_receive_frames

write_assigns_frames_strictly_by_capacity

write_starts_next_writer_when_previous_file_capacity_is_reached

write_allows_multiple_concurrent_writers

write_retains_frames_after_successful_write

write_releases_frames_only_after_finalize

write_retries_from_first_frame_after_write_error

write_retries_from_first_frame_after_finalize_error

write_fails_after_max_retries

write_emits_finalize_results_in_file_order

write_frees_committed_future_file_before_previous_result_is_ready

write_handles_partial_final_file

write_does_not_create_empty_final_file

write_fails_when_write_files_run_out

write_rejects_file_larger_than_global_budget

write_releases_budget_on_drop
```

---

# 74. Tests budget

```text
shared_budget_never_overallocates

scheduler_can_grow_when_capacity_is_released

scheduler_can_shrink

drop_returns_all_capacity

multiple_read_and_write_schedulers_share_budget

waiting_scheduler_is_woken_when_capacity_returns
```

---

# 75. Tests de comportement async

```text
no_busy_loop_when_all_sources_pending

ready_source_does_not_starve_other_sources

work_budget_yields_to_executor

open_future_can_finish_long_before_first_io

blocked_consumer_does_not_prevent_allowed_readers_from_filling_window

blocked_writer_does_not_prevent_input_buffering_until_capacity
```

---

# 76. Benchmarks

Mesurer notamment :

```text
frames par seconde

polls par frame

allocations par frame

allocations par fichier

overhead scheduler avec objets toujours Ready

1 fichier actif

plusieurs fichiers actifs

petits fichiers très nombreux

fichiers avec beaucoup de trames

forte backpressure read

forte backpressure write

beaucoup de open() simultanés

retry write fréquent

plusieurs schedulers partageant un FrameBudget
```

L'objectif est que, sur un workload mémoire trivial, le coût du scheduler soit suffisamment faible pour ne pas devenir le facteur dominant.

---

# 77. Organisation du crate

Structure volontairement simple :

```text
src/
    lib.rs
    traits.rs
    config.rs
    error.rs
    budget.rs
    read.rs
    write.rs
```

Éviter tant qu'ils ne sont pas nécessaires :

```text
engine/
strategy/
policy/
state_machine/
direction/
core/
runtime/
```

Une duplication légère entre `read.rs` et `write.rs` est préférable à une abstraction générique opaque.

---

# 78. API publique cible

L'utilisateur normal devrait principalement voir :

```rust
FrameBudget
Window
SchedulerConfig

ReadFile
ReadScheduler

WriteFile
FrameWriter
WriteScheduler

SchedulerError
ContractError
```

Pas davantage en V1.

---

# 79. API utilisateur Read

Exemple conceptuel :

```rust
let scheduler = ReadScheduler::new(
    files,
    budget,
    config,
);

while let Some(frame) = scheduler.next().await {
    let frame = frame?;
    consume(frame);
}
```

Le stream `files` peut continuer à produire des fichiers pendant l'exécution.

Le scheduler ne nécessite pas de connaître tous les fichiers à l'avance.

---

# 80. API utilisateur Write

Exemple conceptuel :

```rust
let scheduler = WriteScheduler::new(
    frames,
    files,
    budget,
    config,
);

while let Some(result) = scheduler.next().await {
    let result = result?;
    handle_completed_file(result);
}
```

Les fichiers destinations peuvent également être fournis à la volée.

---

# 81. Symétrie finale

Le modèle peut être résumé ainsi.

## Read

```text
ordered Stream<File>
        │
        │ open ahead
        ▼
prepared readers
        │
        │ contiguous logical window
        ▼
bounded scheduler buffers
        │
        ▼
ordered Stream<T>
```

## Write

```text
ordered Stream<T>
+
ordered Stream<File>
        │
        │ open ahead
        ▼
scheduler retained buffers
        │
        │ concurrent writers
        ▼
finalize results
        │
        ▼
ordered Stream<R>
```

---

# 82. Frontière finale de responsabilité

`io-scheduler` sait uniquement :

```text
combien de trames existent
combien peuvent être engagées
dans quel ordre elles appartiennent aux fichiers
quels fichiers peuvent être ouverts
quels readers/writers peuvent progresser
quand les données peuvent être libérées
dans quel ordre les résultats doivent être exposés
```

Il ne sait jamais :

```text
ce qu'est réellement un fichier
ce qu'est réellement une trame
comment les données sont représentées
comment un File transforme ou découpe ses données
pourquoi la fenêtre vaut N
ce que signifie le résultat R
```

Cette ignorance est une propriété fondamentale de l'architecture, pas une limitation.

# 83. Amendements V1 du 17 septembre 2026

Ces dispositions prévalent sur les sections précédentes en cas de conflit.

## 83.1 Écriture sans retry

`SchedulerConfig::max_retries == 0` désactive le journal de replay. Chaque
trame est détruite dès que `poll_write` retourne `Ready(Ok(()))`, sans attendre
`finalize`. Une trame dont l'écriture est `Pending` reste détenue. Une erreur
open/write/finalize est immédiatement fatale. Le nombre de trames affectées au
fichier est suivi indépendamment du nombre encore détenu.

Dans ce mode, `frame_capacity()` peut dépasser le budget global : le fichier
est écrit progressivement dans la fenêtre locale. La contrainte de capacité
entière et la réservation atomique par fichier s'appliquent uniquement lorsque
`max_retries > 0`. Aucun `Clone<T>` ou `Arc<T>` n'est imposé.

## 83.2 Routage des réveils

Chaque source et chaque slot reçoit un waker dédié. Les réveils sont dédupliqués
et routés vers une file de travail. Un composant ayant retourné `Pending` n'est
pas repollé sans réveil. Un composant ayant retourné `Ready` peut continuer
localement tant que ses quotas le permettent. Une modification de quota ne
repoll pas une opération déjà `Pending`. Aucun scan de tous les slots n'est
nécessaire pour traiter un réveil isolé. La file de réveils ne constitue ni une
tâche interne ni un channel de transfert de trames.

## 83.3 Budget partagé

Le budget utilise une attente FIFO. Une demande contient un minimum vital et
une cible. Le budget réserve la capacité avant de réveiller le demandeur ; seuls
les demandeurs satisfaits sont réveillés. Les demandes de lecture utilisent des
blocs allant jusqu'à 32 trames pour éviter les acquisitions unitaires sous
contention. Une capacité totale ou une cible inférieure à 32 reste utilisable.

Avec retry, le scheduler réserve un fichier entier avant sa première trame.
Lorsqu'une extension ne peut être obtenue, la capacité locale inutilisée est
rendue avant l'attente. Cela évite l'interblocage entre journaux partiels.

## 83.4 Stockage et épinglage

Les trames lues sont stockées dans un anneau plat par scheduler, indexé selon
leur position logique. Les slots contiennent leurs compteurs et leur position,
sans buffer de trames alloué par fichier. L'anneau conserve sa capacité maximale
atteinte ; une réduction de fenêtre réduit le budget engagé, sans réallouer.

Les futures d'ouverture, readers, writers et streams d'entrée peuvent être
`!Unpin`. Les états I/O utilisent une projection de pin sûre. Leurs emplacements
épinglés et les sous-wakers sont réutilisés entre fichiers. L'allocation initiale
par emplacement actif est assumée : `pin_project!` seul ne stabilise pas les
éléments d'un `VecDeque` susceptible de déplacer ses éléments.

## 83.5 Précisions d'exécution

Un stream n'avance que lorsqu'il est pollé. Sans tâche interne, aucune activité
ne continue pendant que le consumer ne poll pas le scheduler. Lors d'un poll,
les sources autorisées peuvent remplir la fenêtre même si la première source
est bloquée. Un contrôle EOF supplémentaire après la dernière trame est
nécessaire pour détecter le surplus ; il n'autorise pas de buffering supplémentaire.

Une réduction de fenêtre respecte les trames déjà engagées et les ressources
déjà ouvertes. Elle s'applique progressivement, sans perte ni annulation de
fichier partiel. Une erreur est émise une seule fois puis le stream est terminé.

Les files de résultats finalisés sont aussi bornées par l'horizon logique de
fichiers découverts. La publication sur crates.io reste une opération distincte
de la création du dépôt privé et de la préparation du package Cargo.
