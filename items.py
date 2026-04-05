from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/items", tags=["items"])


class Item(BaseModel):
    name: str = Field(..., example="Widget")
    price: float = Field(..., example=9.99)
    in_stock: bool = Field(True, example=True)


MOCK_ITEMS: list[Item] = [
    Item(name="Widget", price=9.99),
    Item(name="Gadget", price=49.99, in_stock=False),
]


@router.get("/", summary="List all items")
async def list_items() -> list[Item]:
    """Return every item in the store."""
    return MOCK_ITEMS


@router.post("/", summary="Create an item", status_code=201)
async def create_item(item: Item) -> Item:
    """Add a new item and echo it back."""
    MOCK_ITEMS.append(item)
    return item
